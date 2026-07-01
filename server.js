/**
 * CS2 Analytics Dashboard — backend (публичная версия с PostgreSQL)
 * -----------------------------------------------------
 * Прокси к PandaScore API + расчёт аналитической вероятности исхода матча.
 * История прогнозов хранится в облачной БД Supabase (PostgreSQL).
 */

const express = require('express');
const cors = require('cors');
const NodeCache = require('node-cache');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json());

const PANDASCORE_TOKEN = process.env.PANDASCORE_TOKEN;

// Настройка пула подключений к PostgreSQL (Supabase)
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false } // Обязательно для Supabase
});

pool.on('connect', () => {
  console.log('✅ Подключение к PostgreSQL (Supabase) установлено');
});

const PLATFORM_BASE_URL = 'https://api.pandascore.co';
const CSGO_BASE_URL = 'https://api.pandascore.co/csgo';

if (!PANDASCORE_TOKEN) {
  console.warn('⚠️  PANDASCORE_TOKEN не задан. Установите переменную окружения перед запуском.');
}

process.on('unhandledRejection', (reason) => {
  console.error('Необработанный rejection (процесс продолжает работу):', reason);
});
process.on('uncaughtException', (err) => {
  console.error('Необработанное исключение (процесс продолжает работу):', err);
});

const REFRESH_INTERVAL_MS = 15 * 60 * 1000; // 15 минут
const MATCHES_TO_PRERENDER = 20;

const publicState = {
  upcomingMatches: [],
  analyzedMatches: [],
  lastUpdatedAt: null,
  lastError: null,
  isRefreshing: false,
};

const teamCache = new NodeCache({ stdTTL: 1800 });

async function pandaFetch(path, params = {}, baseUrl = CSGO_BASE_URL, attempt = 1) {
  const MAX_ATTEMPTS = 3;
  const url = new URL(baseUrl + path);
  url.searchParams.set('token', PANDASCORE_TOKEN);
  Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));

  let res;
  try {
    res = await fetch(url.toString());
  } catch (networkErr) {
    if (attempt < MAX_ATTEMPTS) {
      await sleep(attempt * 1000);
      return pandaFetch(path, params, baseUrl, attempt + 1);
    }
    throw new Error(`Сетевая ошибка при запросе к PandaScore после ${MAX_ATTEMPTS} попыток: ${networkErr.message}`);
  }

  if (!res.ok) {
    const text = await res.text();
    const retryable = res.status === 429 || res.status >= 500;
    if (retryable && attempt < MAX_ATTEMPTS) {
      await sleep(attempt * 1000);
      return pandaFetch(path, params, baseUrl, attempt + 1);
    }
    throw new Error(`PandaScore ${res.status}: ${text}`);
  }
  return res.json();
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/* ===================== РАСЧЁТ АНАЛИТИКИ ===================== */

function computeForm(matches, teamId, limit = 10) {
  const relevant = matches
    .filter(m => m.winner_id != null)
    .slice(0, limit);

  if (relevant.length === 0) {
    return {
      winRate: 0.5, sample: 0, avgRoundDiff: 0, roundDataAvailable: false,
      momentum: 0, recentForm: [], lastMatchAt: null,
    };
  }

  let wins = 0;
  let roundDiffSum = 0;
  let roundDiffCount = 0;
  const recentForm = [];

  relevant.forEach(m => {
    const won = m.winner_id === teamId;
    if (won) wins += 1;
    recentForm.push(won ? 'W' : 'L');

    (m.games || []).forEach(g => {
      if (g.complete === false) return;
      const candidates = g.results || g.round_score || [];
      const myScore = pickScore(candidates, teamId);
      const oppScore = pickOpponentScore(candidates, teamId);
      if (typeof myScore === 'number' && typeof oppScore === 'number') {
        roundDiffSum += myScore - oppScore;
        roundDiffCount += 1;
      }
    });
  });

  let momentum = 0;
  let momentumAvailable = false;
  if (relevant.length >= 4) {
    const half = Math.floor(relevant.length / 2);
    const recentHalf = relevant.slice(0, half);
    const olderHalf = relevant.slice(half);
    const recentWinRate = recentHalf.filter(m => m.winner_id === teamId).length / recentHalf.length;
    const olderWinRate = olderHalf.filter(m => m.winner_id === teamId).length / olderHalf.length;
    momentum = recentWinRate - olderWinRate;
    momentumAvailable = true;
  }

  return {
    winRate: wins / relevant.length,
    sample: relevant.length,
    avgRoundDiff: roundDiffCount ? roundDiffSum / roundDiffCount : 0,
    roundDataAvailable: roundDiffCount > 0,
    recentForm,
    momentum,
    momentumAvailable,
    lastMatchAt: relevant[0]?.begin_at || relevant[0]?.end_at || null,
  };
}

function pickScore(resultsArr, teamId) {
  const entry = (resultsArr || []).find(r => r.team_id === teamId);
  return entry ? entry.score : undefined;
}

function pickOpponentScore(resultsArr, teamId) {
  const entry = (resultsArr || []).find(r => r.team_id !== teamId);
  return entry ? entry.score : undefined;
}

function computeHeadToHead(allMatchesA, teamAId, teamBId) {
  const h2h = allMatchesA.filter(m =>
    (m.opponents || []).some(o => o.opponent?.id === teamBId)
  );
  if (h2h.length === 0) return { winRate: null, sample: 0, history: [], mostRecentAt: null };

  const wins = h2h.filter(m => m.winner_id === teamAId).length;
  const history = h2h.slice(0, 5).map(m => ({
    date: m.begin_at || m.end_at || null,
    winnerIsTeamA: m.winner_id === teamAId,
    tournament: m.tournament?.name || m.league?.name || null,
  }));

  return {
    winRate: wins / h2h.length,
    sample: h2h.length,
    history,
    mostRecentAt: history[0]?.date || null,
  };
}

function describeTournamentStrength(tournament) {
  const tier = tournament?.tier || null;
  const prizepoolRaw = tournament?.prizepool || null;
  const type = tournament?.type || null;
  const tierLabels = { s: 'S-tier (топ)', a: 'A-tier', b: 'B-tier', c: 'C-tier', d: 'D-tier / квалификатор' };

  let prizepoolAmount = null;
  if (prizepoolRaw) {
    const match = String(prizepoolRaw).match(/^([\d,]+)/);
    if (match) prizepoolAmount = Number(match[1].replace(/,/g, ''));
  }

  return {
    tier,
    tierLabel: tier ? (tierLabels[tier.toLowerCase()] || tier) : 'неизвестно',
    prizepoolRaw,
    prizepoolAmount,
    format: type === 'offline' ? 'LAN (офлайн)' : type === 'online' ? 'онлайн' : 'неизвестно',
    region: tournament?.region || null,
  };
}

function estimateProbability(formA, formB, h2h) {
  const formScore = 0.5 + (formA.winRate - formB.winRate) / 2;

  let roundWeight = 0.20;
  let roundScore = 0.5;
  if (formA.roundDataAvailable && formB.roundDataAvailable) {
    const roundDiffNorm = Math.tanh((formA.avgRoundDiff - formB.avgRoundDiff) / 8);
    roundScore = 0.5 + roundDiffNorm / 2;
  } else {
    roundWeight = 0;
  }

  let h2hScore = 0.5;
  let h2hWeight = 0;
  if (h2h.sample >= 2) {
    h2hScore = h2h.winRate;
    h2hWeight = Math.min(h2h.sample / 6, 1);
  }

  let momentumWeight = 0.15;
  let momentumScore = 0.5;
  if (formA.momentumAvailable && formB.momentumAvailable) {
    const momentumDiff = formA.momentum - formB.momentum;
    momentumScore = 0.5 + Math.tanh(momentumDiff) / 2;
  } else {
    momentumWeight = 0;
  }

  const baseWeight = 0.45;
  const h2hFinalWeight = 0.20 * h2hWeight;
  const totalWeight = baseWeight + roundWeight + h2hFinalWeight + momentumWeight;

  const blended =
    (formScore * baseWeight + roundScore * roundWeight + h2hScore * h2hFinalWeight + momentumScore * momentumWeight) /
    totalWeight;

  return Math.min(0.92, Math.max(0.08, blended));
}

function computeConfidence(sampleA, sampleB, h2hSample, probabilityGapPercent) {
  const minSample = Math.min(sampleA, sampleB);
  const sampleScore = Math.min(minSample / 10, 1) * 50;
  const h2hScore = Math.min(h2hSample / 4, 1) * 20;
  const gapScore = Math.min(probabilityGapPercent / 84, 1) * 30;
  const score = Math.round(sampleScore + h2hScore + gapScore);

  let label;
  if (score >= 70) label = 'высокая';
  else if (score >= 40) label = 'средняя';
  else label = 'низкая — мало данных или близкий матч';

  return { score, label };
}

async function analyzeMatch(matchId) {
  const match = await pandaFetch(`/matches/${matchId}`, {}, PLATFORM_BASE_URL);

  if (!Array.isArray(match.opponents)) {
    const err = new Error('Ответ PandaScore не содержит поле opponents для этого матча — возможно, матч ещё не сформирован или удалён.');
    err.statusCode = 502;
    throw err;
  }

  const [teamA, teamB] = match.opponents.map(o => o.opponent);

  if (!teamA || !teamB) {
    const err = new Error('Матч без двух определённых команд (TBD)');
    err.statusCode = 400;
    throw err;
  }

  let matchesA = teamCache.get(`team:${teamA.id}`);
  if (!matchesA) {
    matchesA = await pandaFetch(`/teams/${teamA.id}/matches`, { per_page: 20, sort: '-begin_at' }, PLATFORM_BASE_URL);
    teamCache.set(`team:${teamA.id}`, matchesA);
  }

  let matchesB = teamCache.get(`team:${teamB.id}`);
  if (!matchesB) {
    matchesB = await pandaFetch(`/teams/${teamB.id}/matches`, { per_page: 20, sort: '-begin_at' }, PLATFORM_BASE_URL);
    teamCache.set(`team:${teamB.id}`, matchesB);
  }

  const formA = computeForm(matchesA, teamA.id);
  const formB = computeForm(matchesB, teamB.id);
  const h2h = computeHeadToHead(matchesA, teamA.id, teamB.id);

  const probA = estimateProbability(formA, formB, h2h);
  const probTeamAPct = Math.round(probA * 100);
  const probTeamBPct = Math.round((1 - probA) * 100);
  const confidence = computeConfidence(
    formA.sample, formB.sample, h2h.sample, Math.abs(probTeamAPct - probTeamBPct)
  );

  const tournamentStrength = describeTournamentStrength(match.tournament);

  const maps = (match.games || []).map(g => ({
    position: g.position,
    status: g.status || null,
    winnerId: g.winner?.id ?? null,
    winnerIsTeamA: g.winner?.id != null ? g.winner.id === teamA.id : null,
  }));

  const result = {
    match: {
      id: match.id,
      name: match.name,
      beginAt: match.begin_at,
      tournament: match.tournament?.name,
      league: match.league?.name,
      numberOfGames: match.number_of_games,
      maps,
    },
    tournamentStrength,
    teamA: { id: teamA.id, name: teamA.name, image: teamA.image_url, form: formA },
    teamB: { id: teamB.id, name: teamB.name, image: teamB.image_url, form: formB },
    headToHead: h2h,
    probability: { teamA: probTeamAPct, teamB: probTeamBPct },
    confidence,
    disclaimer:
      'Оценка построена на статистике публичных матчей (форма, разница раундов, личные встречи, тренд формы). ' +
      'Детальная статистика по картам и экономике раундов недоступна на бесплатном плане PandaScore — модель ' +
      'честно понижает вес тех факторов, для которых не нашлось данных, вместо того чтобы использовать ноль ' +
      'как нейтральное значение. Это аналитическая модель на основе истории, а не гарантированный прогноз ' +
      'и не финансовая рекомендация.',
  };

  return result;
}

/* ===================== ФОНОВЫЙ ВОРКЕР ОБНОВЛЕНИЯ ===================== */

async function refreshCycle() {
  if (publicState.isRefreshing) return;
  publicState.isRefreshing = true;

  try {
    const upcoming = await pandaFetch('/matches/upcoming', { per_page: 30, sort: 'begin_at' });

    const toAnalyze = upcoming.slice(0, MATCHES_TO_PRERENDER);
    const analyzed = [];

    for (const m of toAnalyze) {
      try {
        const result = await analyzeMatch(m.id);
        analyzed.push(result);
        
        // Сохраняем прогноз в БД
        await savePrediction({
          id: result.match.id,
          team1: result.teamA.name,
          team2: result.teamB.name,
          predictedWinner: result.probability.teamA >= result.probability.teamB ? result.teamA.name : result.teamB.name,
          confidenceScore: result.confidence.score
        });
      } catch (e) {
        console.warn(`Пропущен матч ${m.id} при фоновом обновлении: ${e.message}`);
      }
    }

    analyzed.sort((a, b) => b.confidence.score - a.confidence.score);

    // Проверяем завершенные матчи и обновляем результаты в БД
    await resolveFinishedPredictions(upcoming);

    publicState.upcomingMatches = upcoming;
    publicState.analyzedMatches = analyzed;
    publicState.lastUpdatedAt = new Date().toISOString();
    publicState.lastError = null;
  } catch (e) {
    console.error('Ошибка фонового обновления:', e.message);
    publicState.lastError = e.message;
  } finally {
    publicState.isRefreshing = false;
  }
}

function startBackgroundRefresh() {
  refreshCycle();
  setInterval(refreshCycle, REFRESH_INTERVAL_MS);
}

const STALE_THRESHOLD_MS = REFRESH_INTERVAL_MS * 3;

function isDataStale() {
  if (!publicState.lastUpdatedAt) return true;
  return Date.now() - new Date(publicState.lastUpdatedAt).getTime() > STALE_THRESHOLD_MS;
}

/* ===================== РАБОТА С БД (PostgreSQL) ===================== */

async function savePrediction(matchData) {
  const query = `
    INSERT INTO predictions (match_id, team1, team2, predicted_winner, confidence_score)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (match_id) DO UPDATE 
    SET team1 = $2, team2 = $3, predicted_winner = $4, confidence_score = $5;
  `;
  const values = [
    String(matchData.id), 
    matchData.team1, 
    matchData.team2, 
    matchData.predictedWinner, 
    matchData.confidenceScore
  ];
  
  try {
    await pool.query(query, values);
  } catch (err) {
    console.error('❌ Ошибка сохранения прогноза в БД:', err.message);
  }
}

async function resolveFinishedPredictions(recentlySeenMatches) {
  try {
    for (const m of recentlySeenMatches) {
      if (m.winner_id == null) continue;

      const winnerName = m.winner_id === m.opponents?.[0]?.opponent?.id
        ? m.opponents[0].opponent.name
        : m.opponents?.[1]?.opponent?.name || null;

      if (winnerName) {
        await pool.query(`
          UPDATE predictions 
          SET resolved = TRUE, 
              actual_winner = $1, 
              is_correct = (predicted_winner = $1)
          WHERE match_id = $2 AND resolved = FALSE;
        `, [winnerName, String(m.id)]);
      }
    }
  } catch (err) {
    console.error('❌ Ошибка обновления результатов матчей в БД:', err.message);
  }
}

async function getAccuracyStats() {
  try {
    const res = await pool.query(`
      SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN is_correct = true THEN 1 END) as correct,
        COUNT(CASE WHEN confidence_score >= 70 AND is_correct = true THEN 1 END) as correct_high,
        COUNT(CASE WHEN confidence_score >= 70 THEN 1 END) as total_high,
        COUNT(CASE WHEN confidence_score >= 40 AND confidence_score < 70 AND is_correct = true THEN 1 END) as correct_mid,
        COUNT(CASE WHEN confidence_score >= 40 AND confidence_score < 70 THEN 1 END) as total_mid,
        COUNT(CASE WHEN confidence_score < 40 AND is_correct = true THEN 1 END) as correct_low,
        COUNT(CASE WHEN confidence_score < 40 THEN 1 END) as total_low
      FROM predictions 
      WHERE resolved = TRUE;
    `);

    const row = res.rows[0];
    if (row.total == 0) {
      return { totalResolved: 0, accuracy: null, byConfidence: {} };
    }

    const byConfidence = {};
    if (row.total_high > 0) {
      byConfidence['высокая'] = {
        sample: row.total_high,
        accuracy: Math.round((row.correct_high / row.total_high) * 100)
      };
    }
    if (row.total_mid > 0) {
      byConfidence['средняя'] = {
        sample: row.total_mid,
        accuracy: Math.round((row.correct_mid / row.total_mid) * 100)
      };
    }
    if (row.total_low > 0) {
      byConfidence['низкая'] = {
        sample: row.total_low,
        accuracy: Math.round((row.correct_low / row.total_low) * 100)
      };
    }

    return {
      totalResolved: row.total,
      accuracy: Math.round((row.correct / row.total) * 100),
      byConfidence
    };
  } catch (err) {
    console.error('❌ Ошибка получения статистики:', err.message);
    return { totalResolved: 0, accuracy: null, byConfidence: {} };
  }
}

/* ===================== ЭНДПОИНТЫ ===================== */

app.get('/api/matches/:id/analysis', (req, res) => {
  const found = publicState.analyzedMatches.find(m => String(m.match.id) === String(req.params.id));
  if (!found) {
    return res.status(404).json({
      error: 'Этот матч пока не входит в число предрассчитанных — аналитика доступна только для топ-' + MATCHES_TO_PRERENDER + ' ближайших матчей.',
    });
  }
  res.json(found);
});

app.get('/api/status', (req, res) => {
  res.json({
    lastUpdatedAt: publicState.lastUpdatedAt,
    isRefreshing: publicState.isRefreshing,
    lastError: publicState.lastError,
    matchesTracked: publicState.analyzedMatches.length,
    refreshIntervalMs: REFRESH_INTERVAL_MS,
    isStale: isDataStale(),
  });
});

app.get('/api/accuracy', async (req, res) => {
  try {
    const stats = await getAccuracyStats();
    res.json(stats);
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch accuracy' });
  }
});

app.get('/health', (req, res) => res.json({ ok: true }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`CS2 analytics backend running on port ${PORT}`);
  startBackgroundRefresh();
});
