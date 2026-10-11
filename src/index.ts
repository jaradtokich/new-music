/**
 * New Music Scraper - High Capacity Multi-Source Engine
 * 
 * Nightly automated runner harvesting underground, indie,
 * and undiscovered music tracks across three major sources:
 * 1. Audius (decentralized indie catalog, play counts < 35k)
 * 2. Deezer (indie subgenre sweeps, label searches, genre charts)
 * 3. iTunes / Apple Music (underground niche queries, 30s high-fidelity previews)
 * 
 * Deduplicates in-memory and upserts into Supabase 'public.tracks'.
 * Existing tracks update on ID conflict so the database stays tidy.
 */

import { createClient } from '@supabase/supabase-js';
import ws from 'ws';

// Ensure WebSocket is defined across Node environments
if (typeof (globalThis as any).WebSocket === 'undefined') {
  (globalThis as any).WebSocket = ws;
}

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('❌ Error: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment.');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
  realtime: { transport: ws },
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

// Audius Genres
const AUDIUS_GENRES = [
  'Alternative', 'Ambient', 'Classical', 'Country', 'Electronic',
  'Experimental', 'Folk', 'Hip-Hop/Rap', 'Indie', 'Jazz',
  'Metal', 'Pop', 'Punk', 'R&B/Soul', 'Reggae', 'Rock',
  'Techno', 'Trap', 'House', 'Lo-Fi', 'Hyperpop', 'Acoustic',
  'Deep House', 'Chillwave', 'World'
];

// Deezer Subgenres
const DEEZER_QUERIES = [
  'indie rock', 'dream pop', 'math rock', 'shoegaze', 'post-punk',
  'midwest emo', 'bedroom pop', 'vaporwave', 'synthwave', 'lo-fi beats',
  'indie folk', 'neo-psychedelia', 'ambient electronic', 'hyperpop',
  'afrobeats indie', 'alt country', 'jazz fusion', 'trip hop indie',
  'garage rock indie', 'chillwave', 'downtempo', 'electropop indie',
  'art pop', 'folk punk', 'slowcore', 'noise pop', 'indie surf',
  'darkwave', 'glitch hop', 'indie electronic'
];

// Deezer Indie Labels (multi-tag searches)
const DEEZER_LABELS = [
  'sub pop', '4ad', 'warp', 'rough trade', 'matador', 'ninja tune',
  'fat cat', 'ghostly international', 'secretly canadian'
];

// Deezer Genre Editorial Charts
const DEEZER_GENRE_CHARTS = [
  { id: 85, name: 'Alternative' },
  { id: 152, name: 'Rock' },
  { id: 466, name: 'Folk' },
  { id: 106, name: 'Electro' },
  { id: 144, name: 'Reggae' },
  { id: 129, name: 'Jazz' },
  { id: 116, name: 'Rap/Hip Hop' },
  { id: 165, name: 'R&B' },
  { id: 464, name: 'Metal' }
];

// iTunes Underground & Indie Subgenres
const ITUNES_QUERIES = [
  'bedroom pop', 'midwest emo', 'dream pop', 'shoegaze', 'math rock',
  'lo-fi indie', 'synthwave indie', 'darkwave', 'post-punk indie',
  'folk punk', 'slowcore', 'noise pop', 'alt country indie',
  'hyperpop indie', 'chillwave', 'garage rock indie', 'art pop indie',
  'indie surf', 'glitch hop indie', 'indie electronic', 'ambient indie',
  'chamber pop', 'twee pop', 'riot grrrl', 'emo revival'
];

async function getAudiusBaseUrl(): Promise<string> {
  try {
    const res = await fetch('https://api.audius.co', { headers: { 'User-Agent': 'Mozilla/5.0' } });
    const json = await res.json();
    return json?.data?.[0] || 'https://api.audius.co';
  } catch {
    return 'https://api.audius.co';
  }
}

/**
 * 1. Audius Scraper: Trending underground + 25 genre streams with pagination
 */
async function scrapeAudius(): Promise<TrackRecord[]> {
  console.log('🎧 [1/3] Scraping Audius across genres and underground charts...');
  const tracks: TrackRecord[] = [];
  const baseUrl = await getAudiusBaseUrl();

  // A. Underground trending (pagination: 0, 50, 100, 150)
  for (const offset of [0, 50, 100, 150]) {
    try {
      const url = `${baseUrl}/v1/tracks/trending/underground?app_name=new-music-scraper&limit=50&offset=${offset}`;
      const res = await fetch(url, { headers: { Accept: 'application/json', 'User-Agent': 'Mozilla/5.0' } });
      if (res.ok) {
        const json = await res.json();
        const items = json?.data || [];
        for (const item of items) {
          if (!item.id || !item.title || !item.user?.name) continue;
          if ((item.play_count || 0) > 35000) continue;

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
      await sleep(80);
    } catch (e) {
      console.warn(`Audius underground offset ${offset} failed:`, e);
    }
  }

  // B. Loop across all 25 genres (offsets 0, 50, 100)
  for (const g of AUDIUS_GENRES) {
    for (const offset of [0, 50, 100]) {
      try {
        const url = `${baseUrl}/v1/tracks/trending?app_name=new-music-scraper&genre=${encodeURIComponent(g)}&limit=50&offset=${offset}`;
        const res = await fetch(url, { headers: { Accept: 'application/json', 'User-Agent': 'Mozilla/5.0' } });
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
        await sleep(80);
      } catch (e) {
        console.warn(`Audius genre ${g} offset ${offset} error:`, e);
      }
    }
  }

  console.log(`✅ Audius: Harvested ${tracks.length} raw tracks.`);
  return tracks;
}

/**
 * 2. Deezer Scraper: Subgenres, indie label tags, and editorial genre charts
 */
async function scrapeDeezer(): Promise<TrackRecord[]> {
  console.log(`📻 [2/3] Scraping Deezer queries, labels, and genre charts...`);
  const tracks: TrackRecord[] = [];

  // A. Subgenre keyword sweeps
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
          if (rank > 450000) continue;

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
              artist: item.artist.link || '',
            },
            source: 'deezer',
            genre: genreTag,
            genres: [genreTag, q],
            popularity: Math.min(100, Math.round((rank / 450000) * 100)),
            scraped_at: new Date().toISOString(),
          });
        }
        await sleep(100);
      } catch (e) {
        console.warn(`Deezer query "${q}" index ${index} error:`, e);
      }
    }
  }

  // B. Indie label searches (multi-tag)
  for (const label of DEEZER_LABELS) {
    try {
      const url = `https://api.deezer.com/search?q=label:%22${encodeURIComponent(label)}%22&limit=50`;
      const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      if (res.ok) {
        const json = await res.json();
        for (const item of (json?.data || [])) {
          if (!item.id || !item.title || !item.artist?.name || !item.preview) continue;
          const rank = item.rank || 0;
          const artistName = String(item.artist.name).trim();

          tracks.push({
            id: `deezer_${item.id}`,
            title: String(item.title).trim(),
            artist: artistName,
            artist_id: `deezer_${item.artist.id || sanitizeId(artistName)}`,
            preview_url: item.preview,
            artwork_url: item.album?.cover_medium || item.album?.cover_big || null,
            external_urls: { deezer: item.link || '', artist: item.artist.link || '' },
            source: 'deezer',
            genre: 'Indie',
            genres: ['Indie', label],
            popularity: Math.min(100, Math.round((rank / 800000) * 100)),
            scraped_at: new Date().toISOString(),
          });
        }
      }
      await sleep(100);
    } catch (e) {
      console.warn(`Deezer label ${label} error:`, e);
    }
  }

  // C. Genre editorial charts
  for (const g of DEEZER_GENRE_CHARTS) {
    try {
      const url = `https://api.deezer.com/chart/${g.id}/tracks?limit=50`;
      const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      if (res.ok) {
        const json = await res.json();
        for (const item of (json?.data || [])) {
          if (!item.id || !item.title || !item.artist?.name || !item.preview) continue;
          const artistName = String(item.artist.name).trim();
          tracks.push({
            id: `deezer_${item.id}`,
            title: String(item.title).trim(),
            artist: artistName,
            artist_id: `deezer_${item.artist.id || sanitizeId(artistName)}`,
            preview_url: item.preview,
            artwork_url: item.album?.cover_medium || item.album?.cover_big || null,
            external_urls: { deezer: item.link || '', artist: item.artist.link || '' },
            source: 'deezer',
            genre: g.name,
            genres: [g.name],
            popularity: Math.min(100, Math.round(((item.rank || 0) / 900000) * 100)),
            scraped_at: new Date().toISOString(),
          });
        }
      }
      await sleep(100);
    } catch (e) {
      console.warn(`Deezer chart ${g.name} error:`, e);
    }
  }

  console.log(`✅ Deezer: Harvested ${tracks.length} raw tracks.`);
  return tracks;
}

/**
 * 3. iTunes / Apple Music Scraper: High-fidelity indie audio previews & full CORS
 */
async function scrapeItunes(): Promise<TrackRecord[]> {
  console.log(`🍎 [3/3] Scraping iTunes underground indie catalog...`);
  const tracks: TrackRecord[] = [];

  for (const q of ITUNES_QUERIES) {
    try {
      const url = `https://itunes.apple.com/search?term=${encodeURIComponent(q)}&media=music&entity=song&limit=50`;
      const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      if (!res.ok) continue;

      const json = await res.json();
      const items = json?.results || [];

      for (const item of items) {
        if (!item.trackId || !item.trackName || !item.artistName || !item.previewUrl) continue;

        const artistName = String(item.artistName).trim();
        const genre = item.primaryGenreName || q.split(' ')[0] || 'Indie';
        const art = item.artworkUrl100
          ? String(item.artworkUrl100).replace('100x100bb', '600x600bb')
          : null;

        tracks.push({
          id: `itunes_${item.trackId}`,
          title: String(item.trackName).trim(),
          artist: artistName,
          artist_id: `itunes_${item.artistId || sanitizeId(artistName)}`,
          preview_url: item.previewUrl,
          artwork_url: art,
          external_urls: {
            itunes: item.trackViewUrl || '',
            artist: item.artistViewUrl || '',
          },
          source: 'itunes',
          genre,
          genres: [genre, q],
          popularity: 45,
          scraped_at: new Date().toISOString(),
        });
      }
      await sleep(100);
    } catch (e) {
      console.warn(`iTunes query "${q}" error:`, e);
    }
  }

  console.log(`✅ iTunes: Harvested ${tracks.length} raw tracks.`);
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
    await sleep(40);
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
  console.log('🚀 Starting 3-engine high-capacity music discovery scraper at', new Date().toISOString());

  const [audiusTracks, deezerTracks, itunesTracks] = await Promise.all([
    scrapeAudius(),
    scrapeDeezer(),
    scrapeItunes(),
  ]);

  const allTracks = [...audiusTracks, ...deezerTracks, ...itunesTracks];

  // In-memory deduplication by normalized (artist + title) and ID
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

main().catch((err) => {
  console.error('💥 Fatal scraper error:', err);
  process.exit(1);
});
