# New Music Scraper

Automated, lightweight music discovery engine that unearths undiscovered artists across **Audius**, **Reddit** (`r/listentothis`, `r/IndieMusicFeedback`), and **Deezer**.

## Architecture

- **Scheduled via GitHub Actions:** Runs every midnight UTC (and on-demand).
- **Direct Database Sync:** Upserts sanitized tracks directly into Supabase table `public.tracks`.
- **Zero Local Audio Hosting:** Captures stream previews and direct streaming links without high storage costs.

## Secrets Required (in GitHub repo settings)

- `SUPABASE_URL`: Your Supabase project URL (`https://dwvsrfarzlxoamevgrin.supabase.co`)
- `SUPABASE_SERVICE_ROLE_KEY`: Your secret service_role key to write to Supabase.
