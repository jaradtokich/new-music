/**
 * New Music Scraper
 * 
 * Nightly automated runner that harvests underground and indie music from:
 * - Audius (underground & low play-count tracks with playable direct audio)
 * - Reddit (r/listentothis & r/IndieMusicFeedback curated submissions)
 * - Deezer (undiscovered sub-genre searches with 30s previews)
 * 
 * Upserts deduplicated tracks into Supabase 'public.tracks'.
 */

import { createClient } from '@supabase/supabase-js';
import ws from 'ws';

// Ensure WebSocket is defined across Node environments
if (typeof (globalThis as any).WebSocket === 'undefined') {
  (globalThis as any).WebSocket = ws;
}

// Environment credentials passed via GitHub Actions Secrets
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('❌ Error: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment.');
  process.exit(1);
}

// Initialize Supabase admin client with ws transport and non-persistent auth
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
  preview_url: string | null;
  artwork_url: string | null;
  external_urls: Record<string, string>;
  source: string;
  genre: string;
  genres: string[];
  popularity: number;
  scraped_at: string;
}

/**
 * 1. Scrape Audius API for indie / underground songs
 */
async function scrapeAudius(): Promise<TrackRecord[]> {
  console.log('🎧 [1/3] Scraping Audius underground catalog...');
  const tracks: TrackRecord[] = [];

  try {
    const hostRes = await fetch('https://api.audius.co');
    const hostJson = await hostRes.json();
    const hosts: string[] = hostJson.data || ['https://discoveryprovider.audius.co'];
    const baseUrl = hosts[0] || 'https://discoveryprovider.audius.co';

    const url = `${baseUrl}/v1/tracks/trending/underground?app_name=new-music-scraper&limit=50`;
    const res = await fetch(url, {
      headers: { 'Accept': 'application/json' }
    });

    if (!res.ok) {
      console.warn(`⚠️ Audius responded with status ${res.status}`);
      return [];
    }

    const data = await res.json();
    const items = data.data || [];

    for (const item of items) {
      if (!item.id || !item.title || !item.user?.name) continue;

      const playCount = item.play_count || 0;
      if (playCount > 30000) continue;

      const trackId = `audius_${item.id}`;
      const streamUrl = `${baseUrl}/v1/tracks/${item.id}/stream?app_name=new-music-scraper`;
      const artwork = item.artwork?.['480x480'] || item.artwork?.['150x150'] || null;
      const genreVal = item.genre || 'Indie';

      tracks.push({
        id: trackId,
        title: String(item.title).trim(),
        artist: String(item.user.name).trim(),
        preview_url: streamUrl,
        artwork_url: artwork,
        external_urls: {
          audius: `https://audius.co${item.permalink || ''}`
        },
        source: 'audius',
        genre: genreVal,
        genres: [genreVal],
        popularity: Math.min(100, Math.round((playCount / 30000) * 100)),
        scraped_at: new Date().toISOString()
      });
    }

    console.log(`✅ Audius: Harvested ${tracks.length} underground tracks.`);
  } catch (err) {
    console.error('❌ Failed scraping Audius:', err);
  }

  return tracks;
}

/**
 * 2. Scrape Reddit music curation communities (r/listentothis, r/IndieMusicFeedback)
 */
async function scrapeReddit(): Promise<TrackRecord[]> {
  console.log('📻 [2/3] Scraping Reddit indie music communities...');
  const tracks: TrackRecord[] = [];

  const subreddits = ['listentothis', 'IndieMusicFeedback'];

  for (const sub of subreddits) {
    try {
      const url = `https://www.reddit.com/r/${sub}/hot.json?limit=30`;
      const res = await fetch(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
      });

      if (!res.ok) {
        console.warn(`⚠️ Reddit r/${sub} responded with status ${res.status}`);
        continue;
      }

      const json = await res.json();
      const posts = json?.data?.children || [];

      for (const post of posts) {
        const postData = post.data;
        if (!postData || postData.stickied || postData.is_self) continue;

        const rawTitle: string = postData.title || '';
        const match = rawTitle.match(/^([^-–—]+)\s*[-–—]\s*([^\[\(]+)(?:\[(.*?)\])?/);

        let artist = '';
        let title = '';
        let genre = 'Indie Discovery';

        if (match) {
          artist = match[1].trim();
          title = match[2].trim();
          if (match[3]) genre = match[3].trim();
        } else {
          continue;
        }

        const externalUrl = postData.url || `https://reddit.com${postData.permalink}`;
        const artwork = (postData.thumbnail && postData.thumbnail.startsWith('http')) 
          ? postData.thumbnail 
          : null;

        tracks.push({
          id: `reddit_${postData.id}`,
          title,
          artist,
          preview_url: null,
          artwork_url: artwork,
          external_urls: {
            reddit: `https://reddit.com${postData.permalink}`,
            media: externalUrl
          },
          source: 'reddit',
          genre,
          genres: [genre],
          popularity: Math.min(100, Math.max(5, postData.score || 10)),
          scraped_at: new Date().toISOString()
        });
      }
    } catch (err) {
      console.error(`❌ Failed scraping r/${sub}:`, err);
    }
  }

  console.log(`✅ Reddit: Harvested ${tracks.length} community submissions.`);
  return tracks;
}

/**
 * 3. Scrape Deezer indie queries for tracks with 30s audio previews
 */
async function scrapeDeezer(): Promise<TrackRecord[]> {
  console.log('📡 [3/3] Scraping Deezer for low-rank indie previews...');
  const tracks: TrackRecord[] = [];

  const queries = ['bedroom pop indie', 'math rock underground', 'lo-fi beats fresh', 'synthwave indie'];
  const query = queries[Math.floor(Math.random() * queries.length)];

  try {
    const url = `https://api.deezer.com/search?q=${encodeURIComponent(query)}&limit=40`;
    const res = await fetch(url);

    if (!res.ok) {
      console.warn(`⚠️ Deezer responded with status ${res.status}`);
      return [];
    }

    const json = await res.json();
    const items = json.data || [];

    for (const item of items) {
      if (!item.id || !item.title || !item.artist?.name || !item.preview) continue;

      const rank = item.rank || 0;
      if (rank > 450000) continue;

      const genreVal = query.split(' ')[0] || 'Indie';

      tracks.push({
        id: `deezer_${item.id}`,
        title: String(item.title).trim(),
        artist: String(item.artist.name).trim(),
        preview_url: item.preview,
        artwork_url: item.album?.cover_medium || item.album?.cover_big || null,
        external_urls: {
          deezer: item.link || '',
          artist: item.artist.link || ''
        },
        source: 'deezer',
        genre: genreVal,
        genres: [genreVal],
        popularity: Math.min(100, Math.round((rank / 450000) * 100)),
        scraped_at: new Date().toISOString()
      });
    }

    console.log(`✅ Deezer: Harvested ${tracks.length} indie preview tracks.`);
  } catch (err) {
    console.error('❌ Failed scraping Deezer:', err);
  }

  return tracks;
}

/**
 * Batch Upsert to Supabase
 */
async function saveToDatabase(tracks: TrackRecord[]) {
  if (tracks.length === 0) {
    console.log('ℹ️ No new tracks to insert.');
    return;
  }

  console.log(`💾 Saving ${tracks.length} total tracks to Supabase table 'tracks'...`);

  const batchSize = 50;
  let successCount = 0;
  let hasErrors = false;

  for (let i = 0; i < tracks.length; i += batchSize) {
    const batch = tracks.slice(i, i + batchSize);
    const { error } = await supabase
      .from('tracks')
      .upsert(batch, { onConflict: 'id', ignoreDuplicates: false });

    if (error) {
      hasErrors = true;
      console.error(`❌ Supabase error inserting batch ${i / batchSize + 1}:`, error.message);
    } else {
      successCount += batch.length;
    }
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
  console.log('🚀 Starting nightly music discovery scraper at', new Date().toISOString());

  const [audiusTracks, redditTracks, deezerTracks] = await Promise.all([
    scrapeAudius(),
    scrapeReddit(),
    scrapeDeezer()
  ]);

  const allTracks = [...audiusTracks, ...redditTracks, ...deezerTracks];

  const seen = new Set<string>();
  const uniqueTracks = allTracks.filter(t => {
    const key = `${t.artist.toLowerCase()} - ${t.title.toLowerCase()}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  console.log(`✨ Total collected: ${allTracks.length} | Unique: ${uniqueTracks.length}`);

  await saveToDatabase(uniqueTracks);
  console.log('🏁 Scraper finished cleanly.');
}

main().catch(err => {
  console.error('💥 Fatal scraper error:', err);
  process.exit(1);
});
