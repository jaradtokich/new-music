/**
 * New Music Scraper - High Capacity Multi-Genre Runner
 * 
 * Nightly automated runner that harvests thousands of underground,
 * indie, and undiscovered music tracks across all major genres from:
 * - Audius (underground & low play-count tracks with playable direct audio)
 * - Deezer (undiscovered sub-genre searches with 30s previews)
 * 
 * Upserts deduplicated tracks into Supabase 'public.tracks'.
 * Replaces/updates existing tracks on ID conflict so the database stays tidy.
 */

import { createClient } from '@supabase/supabase-js';
import ws from 'ws';

// Ensure WebSocket is defined across Node environments
if (typeof (globalThis as any).WebSocket === 'undefined') {
  (globalThis as any).WebSocket = ws;
}

// Credentials passed via GitHub Actions Secrets
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('❌ Error: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment.');
  process.exit(1);
}

// Initialize Supabase admin client
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: {
    persistSession: false,
    autoRefreshToken: false,
  },
  realtime: {
    transport: ws,
  },
});

export interface TrackRecord {
  id: string;
  title: string;
  artist: string;
  artist_id: string;
  preview_url: string | null;
  artwork_url: string | null;
  external_urls: Record<string, string>;
  source: string;
  genre: string;
  genres: string[];
  popularity: number;
  scraped_at: string;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function sanitizeId(str: string): string {
  return str.toLowerCase().replace(/[^a-z0-9_]/g, '_').slice(0, 50);
}

function normalizeKey(artist: string, title: string): string {
  const clean = (s: string) => s.toLowerCase().replace(/[^a-z0-9]/g, '').trim();
  return `${clean(artist)}:::${clean(title)}`;
}

// Comprehensive genre list covering all available categories on Audius
const AUDIUS_GENRES = [
  'Alternative', 'Ambient', 'Classical', 'Country', 'Electronic',
  'Experimental', 'Folk', 'Hip-Hop/Rap', 'Indie', 'Jazz',
  'Metal', 'Pop', 'Punk', 'R&B/Soul', 'Reggae', 'Rock',
  'Techno', 'Trap', 'House', 'Lo-Fi', 'Hyperpop', 'Acoustic',
  'Deep House', 'Chillwave', 'World'
];

// Rich variety of subgenres and niche keywords for indie Deezer discovery
const DEEZER_QUERIES = [
  'indie rock', 'dream pop', 'math rock', 'shoegaze', 'post-punk',
  'midwest emo', 'bedroom pop', 'vaporwave', 'synthwave', 'lo-fi beats',
  'indie folk', 'neo-psychedelia', 'ambient electronic', 'hyperpop',
  'afrobeats indie', 'alt country', 'jazz fusion', 'trip hop indie',
  'garage rock indie', 'chillwave', 'downtempo', 'electropop indie',
  'art pop', 'folk punk', 'slowcore', 'noise pop', 'indie surf',
  'darkwave', 'glitch hop', 'indie electronic'
];

async function getAudiusBaseUrl(): Promise<string> {
  try {
    const res = await fetch('https://api.audius.co');
    const json = await res.json();
    return json?.data?.[0] || 'https://discoveryprovider.audius.co';
  } catch {
    return 'https://discoveryprovider.audius.co';
  }
}

/**
 * 1. Scrape Audius API across all available genres with pagination
 */
async function scrapeAudius(): Promise<TrackRecord[]> {
  console.log('🎧 [1/2] Scraping Audius across 25 genres and trending catalogs...');
  const tracks: TrackRecord[] = [];
  const baseUrl = await getAudiusBaseUrl();

  // A. Trending Underground (pages 0, 50, 100)
  for (const offset of [0, 50, 100]) {
    try {
      const url = `${baseUrl}/v1/tracks/trending/underground?app_name=new-music-scraper&limit=50&offset=${offset}`;
      const res = await fetch(url, { headers: { Accept: 'application/json' } });
      if (res.ok) {
        const json = await res.json();
        const items = json?.data || [];
        for (const item of items) {
          if (!item.id || !item.title || !item.user?.name) continue;
          if ((item.play_count || 0) > 35000) continue; // Keep genuinely undiscovered tracks

          const artistName = String(item.user.name).trim();
          const genre = item.genre || 'Underground';
          tracks.push({
            id: `audius_${item.id}`,
            title: String(item.title).trim(),
            artist: artistName,
            artist_id: `audius_${item.user.id || sanitizeId(item.user.handle || artistName)}`,
            preview_url: `${baseUrl}/v1/tracks/${item.id}/stream?app_name=new-music-scraper`,
            artwork_url: item.artwork?.['480x480'] || item.artwork?.['150x150'] || null,
            external_urls: { audius: `https://audius.co${item.permalink || ''}` },
            source: 'audius',
            genre,
            genres: [genre],
            popularity: Math.min(100, Math.round(((item.play_count || 0) / 35000) * 100)),
            scraped_at: new Date().toISOString(),
          });
        }
      }
      await sleep(100);
    } catch (e) {
      console.warn(`Audius underground offset ${offset} failed:`, e);
    }
  }

  // B. Loop across all 25 genres (2 pages each = 100 tracks per genre)
  for (const g of AUDIUS_GENRES) {
    for (const offset of [0, 50]) {
      try {
        const url = `${baseUrl}/v1/tracks/trending?app_name=new-music-scraper&genre=${encodeURIComponent(g)}&limit=50&offset=${offset}`;
        const res = await fetch(url, { headers: { Accept: 'application/json' } });
        if (!res.ok) continue;

        const json = await res.json();
        const items = json?.data || [];
        for (const item of items) {
          if (!item.id || !item.title || !item.user?.name) continue;
          if ((item.play_count || 0) > 35000) continue;

          const artistName = String(item.user.name).trim();
          tracks.push({
            id: `audius_${item.id}`,
            title: String(item.title).trim(),
            artist: artistName,
            artist_id: `audius_${item.user.id || sanitizeId(item.user.handle || artistName)}`,
            preview_url: `${baseUrl}/v1/tracks/${item.id}/stream?app_name=new-music-scraper`,
            artwork_url: item.artwork?.['480x480'] || item.artwork?.['150x150'] || null,
            external_urls: { audius: `https://audius.co${item.permalink || ''}` },
            source: 'audius',
            genre: g,
            genres: [g],
            popularity: Math.min(100, Math.round(((item.play_count || 0) / 35000) * 100)),
            scraped_at: new Date().toISOString(),
          });
        }
        await sleep(100);
      } catch (e) {
        console.warn(`Audius genre ${g} offset ${offset} error:`, e);
      }
    }
  }

  console.log(`✅ Audius: Harvested ${tracks.length} raw tracks across ${AUDIUS_GENRES.length} genres.`);
  return tracks;
}

/**
 * 2. Scrape Deezer indie queries with pagination for tracks with 30s audio previews
 */
async function scrapeDeezer(): Promise<TrackRecord[]> {
  console.log(`📻 [2/2] Scraping Deezer across ${DEEZER_QUERIES.length} indie subgenre queries...`);
  const tracks: TrackRecord[] = [];

  for (const q of DEEZER_QUERIES) {
    for (const index of [0, 50]) {
      try {
        const url = `https://api.deezer.com/search?q=${encodeURIComponent(q)}&limit=50&index=${index}`;
        const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
        if (!res.ok) continue;

        const json = await res.json();
        const items = json?.data || [];

        for (const item of items) {
          if (!item.id || !item.title || !item.artist?.name || !item.preview) continue;
          const rank = item.rank || 0;
          if (rank > 450000) continue; // Keep low-rank / indie discoveries

          const artistName = String(item.artist.name).trim();
          const genreTag = q.split(' ')[0] || 'Indie';

          tracks.push({
            id: `deezer_${item.id}`,
            title: String(item.title).trim(),
            artist: artistName,
            artist_id: `deezer_${item.artist.id || sanitizeId(artistName)}`,
            preview_url: item.preview,
            artwork_url: item.album?.cover_medium || item.album?.cover_big || null,
            external_urls: {
              deezer: item.link || '',
              artist: item.artist.link || ''
            },
            source: 'deezer',
            genre: genreTag,
            genres: [genreTag, q],
            popularity: Math.min(100, Math.round((rank / 450000) * 100)),
            scraped_at: new Date().toISOString(),
          });
        }
        await sleep(120);
      } catch (e) {
        console.warn(`Deezer query "${q}" index ${index} error:`, e);
      }
    }
  }

  console.log(`✅ Deezer: Harvested ${tracks.length} indie preview tracks.`);
  return tracks;
}

/**
 * Batch Upsert to Supabase
 * Uses onConflict: 'id' to UPDATE existing tracks instead of creating duplicates.
 */
async function saveToDatabase(tracks: TrackRecord[]) {
  if (tracks.length === 0) {
    console.log('ℹ️ No tracks to insert.');
    return;
  }

  console.log(`💾 Upserting ${tracks.length} tracks to Supabase 'tracks' table...`);
  const batchSize = 100;
  let successCount = 0;
  let hasErrors = false;

  for (let i = 0; i < tracks.length; i += batchSize) {
    const batch = tracks.slice(i, i + batchSize);
    const { error } = await supabase
      .from('tracks')
      .upsert(batch, { onConflict: 'id', ignoreDuplicates: false });

    if (error) {
      hasErrors = true;
      console.error(`❌ Supabase error inserting batch ${Math.floor(i / batchSize) + 1}:`, error.message);
    } else {
      successCount += batch.length;
    }

    if ((i + batchSize) % 500 === 0 || i + batchSize >= tracks.length) {
      console.log(`   ↳ Progress: ${successCount}/${tracks.length} tracks synced.`);
    }
    await sleep(50);
  }

  console.log(`🎉 Successfully synced ${successCount}/${tracks.length} tracks into Supabase!`);

  if (hasErrors && successCount === 0) {
    throw new Error('All Supabase insert batches failed. Check table schema and column names.');
  }
}

/**
 * Main Execution Entrypoint
 */
async function main() {
  console.log('🚀 Starting high-capacity music discovery scraper at', new Date().toISOString());

  const [audiusTracks, deezerTracks] = await Promise.all([
    scrapeAudius(),
    scrapeDeezer(),
  ]);

  const allTracks = [...audiusTracks, ...deezerTracks];

  // In-memory deduplication by normalized (artist + title) and id
  const seenKeys = new Set<string>();
  const uniqueTracks: TrackRecord[] = [];

  for (const t of allTracks) {
    const key = normalizeKey(t.artist, t.title);
    if (seenKeys.has(key) || seenKeys.has(t.id)) continue;
    seenKeys.add(key);
    seenKeys.add(t.id);
    uniqueTracks.push(t);
  }

  console.log(`📊 Scrape Summary: Total raw collected: ${allTracks.length} | Unique deduplicated: ${uniqueTracks.length}`);

  await saveToDatabase(uniqueTracks);
  console.log('🏁 Scraper finished cleanly.');
}

main().catch(err => {
  console.error('💥 Fatal scraper error:', err);
  process.exit(1);
});
