from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
from typing import List, Optional
import os
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="JIH JobSpy Microservice")
SCRAPE_SECRET = os.environ.get("SCRAPE_SECRET")
executor = ThreadPoolExecutor(max_workers=4)


class ScrapeRequest(BaseModel):
    # ── v2 fields (preferred) ─────────────────────────────────────────────────
    # search_terms: list of job titles/queries to search for
    # locations: list of locations to search in — scraper iterates search_terms × locations
    search_terms: Optional[List[str]] = None
    locations: Optional[List[str]] = None

    # ── legacy fields (backward compat) ──────────────────────────────────────
    keywords: Optional[List[str]] = None          # alias for search_terms
    location: str = "Ottawa, Ontario, Canada"     # single location fallback

    # ── common fields ─────────────────────────────────────────────────────────
    hours_old: int = 168                          # jobs posted in last N hours (default 7 days)
    results_per_keyword: int = 5                  # max results per search term per location
    results_wanted: Optional[int] = None          # v2 alias for results_per_keyword (plan compat)
    sites: List[str] = ["linkedin", "indeed"]
    country: str = "Canada"
    country_indeed: Optional[str] = None          # v2 alias for country

    # ── body-based secret (alternative to x-secret header) ───────────────────
    secret: Optional[str] = None


@app.get("/ping")
async def ping():
    """Instant health check — no scraping."""
    return {"status": "ok"}


@app.get("/health")
async def health():
    return {"status": "ok", "service": "jih-jobspy-microservice"}


def _scrape_keyword_location(
    keyword: str,
    location: str,
    sites: List[str],
    results_per_keyword: int,
    hours_old: int,
    country: str,
) -> List[dict]:
    """Synchronous scrape for one keyword × one location — runs in thread executor."""
    from jobspy import scrape_jobs
    import pandas as pd

    logger.info(f"Scraping: '{keyword}' in '{location}' on {sites}")
    jobs = scrape_jobs(
        site_name=sites,
        search_term=keyword,
        location=location,
        results_wanted=results_per_keyword,
        hours_old=hours_old,
        country_indeed=country,
        linkedin_fetch_description=False,  # skip full desc fetch to reduce timeout risk
        description_format="markdown",
    )

    if jobs is None or len(jobs) == 0:
        logger.info(f"'{keyword}' in '{location}': 0 results")
        return []

    jobs_list = jobs.where(pd.notnull(jobs), None).to_dict(orient="records")
    for job in jobs_list:
        job["search_keyword"] = keyword
        job["search_location"] = location
        if not job.get("job_url"):
            job["job_url"] = job.get("id", "")

    logger.info(f"'{keyword}' in '{location}': {len(jobs_list)} results")
    return jobs_list


@app.post("/scrape")
async def scrape_jobs_endpoint(request: ScrapeRequest, x_secret: Optional[str] = Header(None)):
    # Auth: accept either x-secret header or body secret param
    effective_secret = x_secret or request.secret
    if SCRAPE_SECRET and effective_secret != SCRAPE_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Resolve search terms: prefer v2 search_terms, fall back to legacy keywords
    effective_terms = request.search_terms or request.keywords or []
    if not effective_terms:
        raise HTTPException(status_code=400, detail="Provide search_terms (or keywords)")

    # Resolve locations: prefer v2 locations list, fall back to legacy single location
    effective_locations = request.locations or [request.location]

    # Resolve results per keyword: prefer v2 results_wanted alias
    effective_results = request.results_wanted or request.results_per_keyword

    # Resolve country: prefer v2 country_indeed alias
    effective_country = request.country_indeed or request.country

    all_jobs = []
    errors = []
    loop = asyncio.get_event_loop()

    # Iterate search_terms × locations (v2 dynamic parameterisation)
    for keyword in effective_terms:
        for location in effective_locations:
            try:
                logger.info(f"Queuing: '{keyword}' in '{location}'")
                jobs = await asyncio.wait_for(
                    loop.run_in_executor(
                        executor,
                        _scrape_keyword_location,
                        keyword,
                        location,
                        request.sites,
                        effective_results,
                        request.hours_old,
                        effective_country,
                    ),
                    timeout=60.0,
                )
                all_jobs.extend(jobs)
            except asyncio.TimeoutError:
                logger.warning(f"Timeout: '{keyword}' in '{location}'")
                errors.append({"keyword": keyword, "location": location, "error": "Timeout after 60s"})
            except Exception as e:
                logger.error(f"Failed: '{keyword}' in '{location}': {e}")
                errors.append({"keyword": keyword, "location": location, "error": str(e)})

    # Deduplicate by job_url
    seen_urls: set = set()
    unique_jobs = []
    for job in all_jobs:
        url = str(job.get("job_url") or job.get("id") or "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_jobs.append(job)
        elif not url:
            unique_jobs.append(job)

    logger.info(
        f"Scrape complete: {len(unique_jobs)} unique jobs from "
        f"{len(effective_terms)} terms × {len(effective_locations)} locations, "
        f"{len(errors)} errors"
    )
    return {
        "jobs": unique_jobs,
        "total": len(unique_jobs),
        "errors": errors,
        "keywords_searched": effective_terms,
        "locations_searched": effective_locations,
    }
