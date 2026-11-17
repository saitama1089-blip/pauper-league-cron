# Pauper league and challenge scraper

The scraper runs at minutes **7 and 37** each hour, with one active run and a
30-minute timeout. It submits league and challenge metadata to the existing
Supabase views.

After every non-cancelled attempt, it requests the deck-cache worker. This
includes partial failures that may already have saved some results. The
dispatch uses bounded HTTP retries and fails visibly when GitHub rejects it.

Missing tournament pages (HTTP 404/410) skip retries and browser fallback.
Ordinary HTTP pages use requests first; temporary failures retain the existing
Chrome fallback. Browser challenge pages are not accepted as results.

Database submission errors and runs that submit no results now produce a failed
job. The deck-worker dispatch still runs, and the worker's independent hourly
sweep provides another recovery path. Rows submitted can include existing rows
accepted by the insert views; this metric is not a count of net-new records.

Run offline regression tests with Python 3.11:

```sh
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

GitHub's scheduler can delay jobs. Upstream freshness is monitored separately
from cache completeness by the deck worker; a zero missing-deck count does not
prove the scraper has collected the newest results.
