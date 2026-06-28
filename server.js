/**
 * CS2 Analytics Dashboard — backend (публичная версия)
 * -----------------------------------------------------
 * Прокси к PandaScore API + расчёт аналитической вероятности исхода матча
 * на основе публичной статистики (форма команд, head-to-head, отыгранные раунды).
 *
 * АРХИТЕКТУРА ДЛЯ ПУБЛИЧНОГО ТРАФИКА: ни один запрос от посетителя сайта
 * НЕ обращается к PandaScore напрямую. Вместо этого фоновый воркер (см.
 * startBackgroundRefresh ниже) сам, по таймеру, раз в REFRESH_INTERVAL_MS
 * обновляет общий кэш в памяти сервера. Все HTTP-эндпоинты только читают
 * из этого кэша. Это значит: один посетитель и сто тысяч посетителей
 * расходуют ОДИНАКОВОЕ количество запросов к PandaScore — ноль сверх
 * фонового обновления. Без этого 1000 запросов/час бесплатного плана
 * истощились бы за минуты при сколь-нибудь заметном публичном трафике.
 *
 * Это НЕ "инсайды" и НЕ финансовый совет — только статистический анализ
 * публичных данных. Итоговая вероятность — это оценка на основе истории,
 * а не гарантия исхода. Сайт не принимает и не обрабатывает платежи.
 */

const express = require('express');
const cors = require('cors');
const NodeCache = require('node-cache');

const app = express();
app.use(cors());
app.use(express.json());

const PANDASCORE_TOKEN = process.env.PANDASCORE_TOKEN;

// ВАЖНО: у PandaScore есть два разных семейства путей.
//  - Сущности уровня "вся платформа" (команды, игроки, турниры, серии,
//    лиги) живут БЕЗ префикса игры: /teams/{id}/matches, /players/{id}, ...
//    Это подтверждено официальным индексом эндпоинтов (llms.txt):
//    "Get matches for team" → GET /teams/{teamIdOrSlug}/matches.
//  - Сущности, специфичные для конкретной игры (расписание матчей CS2,
//    статистика игроков/команд CS2, карты, оружие) — С префиксом /csgo/:
//    /csgo/matches/upcoming, /csgo/players/{id}/stats, и т.д.
// Раньше здесь был единственный BASE_URL с /csgo/ для всех запросов —
// из-за этого /teams/{id}/matches превращался в /csgo/teams/{id}/matches,
// которого не существует, и PandaScore отвечал {"error":"Route not found"}.
const PLATFORM_BASE_URL = 'https://api.pandascore.co';
const CSGO_BASE_URL = 'https://api.pandascore.co/csgo';

if (!PANDASCORE_TOKEN) {
  console.warn('⚠️  PANDASCORE_TOKEN не задан. Установите переменную окружения перед запуском.');
}

// ПРИМЕЧАНИЕ ПО БЕЗОПАСНОСТИ: здесь больше нет ACCESS_KEY-защиты на /api,
// потому что эндпоинты публичного сайта (см. ниже) НЕ обращаются к
// PandaScore напрямую — они только читают из общего publicState,
// который обновляет фоновый воркер. Открыть их для всех безопасно по
// самой конструкции: сколько угодно посетителей не увеличивают расход
// квоты PandaScore. Токен PandaScore остаётся только в переменной
// окружения сервера и никогда не передаётся в браузер.

// Сколько матчей из верхней части расписания анализируем фоном. Каждый
// новый матч (без команд в teamCache) стоит до 3 запросов к PandaScore.
// При REFRESH_INTERVAL_MS = 15 минут и MATCHES_TO_PRERENDER = 20 это
// в худшем случае (полностью холодный teamCache) ~60 запросов за цикл,
// то есть 4 цикла в час ≈ 240 запросов/час из лимита 1000 — комфортный
// запас даже с учётом неизбежных повторных запросов по новым командам.
const REFRESH_INTERVAL_MS = 15 * 60 * 1000; // 15 минут
const MATCHES_TO_PRERENDER = 20;

// Общее хранилище в памяти — то, что фактически отдаётся посетителям.
// Нет TTL: протухает не само, а полностью заменяется фоновым воркером.
// Если воркер ещё не сделал первый прогон (сервер только запустился),
// поля остаются null/[] — эндпоинты должны явно сообщать "данные
// готовятся", а не отдавать пустоту как готовый ответ.
const publicState = {
  upcomingMatches: [],
  analyzedMatches: [], // те же матчи + аналитика, отсортированные по уверенности
  lastUpdatedAt: null,
  lastError: null,
  isRefreshing: false,
};

// Кэш истории конкретных команд — здесь TTL уместен, потому что это
// внутренний кэш воркера, а не прямой ответ пользователю.
const teamCache = new NodeCache({ stdTTL: 1800 });

async function pandaFetch(path, params = {}, baseUrl = CSGO_BASE_URL) {
  const url = new URL(baseUrl + path);
  url.searchParams.set('token', PANDASCORE_TOKEN);
  Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));

  const res = await fetch(url.toString());
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`PandaScore ${res.status}: ${text}`);
  }
  return res.json();
}

/* ===================== РАСЧЁТ АНАЛИТИКИ ===================== */

/**
 * Форма команды по последним N матчам: доля побед + средняя разница раундов.
 * Берём только завершённые и полностью обработанные матчи (complete: true
 * на уровне игр), чтобы не тащить недосчитанную статистику.
 */
function computeForm(matches, teamId, limit = 10) {
  // Матч считается завершённым и пригодным к использованию, если у него
  // определён победитель — статус и точное название поля статуса варьируются
  // между версиями API, поэтому winner_id — самый надёжный признак.
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
  const recentForm = []; // 'W' | 'L' по каждому матчу, от новых к старым — для визуальной полосы формы

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

  // Momentum: сравниваем винрейт первой половины выборки (более свежие
  // матчи, т.к. список отсортирован от новых к старым) со второй половиной
  // (более старые). Положительное значение = команда набирает форму,
  // отрицательное = теряет. Считаем только если выборка достаточно большая,
  // чтобы обе половины имели смысл — иначе оставляем momentum на 0 (нейтрально)
  // и явно помечаем, что тренд не определён.
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
    recentForm, // например ['W','W','L','W','L'] — для отображения полосой
    momentum, // от -1 до 1, разница винрейта свежей половины и старой
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

/**
 * Head-to-head: доля побед teamA над teamB в их личных встречах, плюс
 * хронология последних встреч (для отображения динамики, не только числа).
 */
function computeHeadToHead(allMatchesA, teamAId, teamBId) {
  const h2h = allMatchesA.filter(m =>
    (m.opponents || []).some(o => o.opponent?.id === teamBId)
  );
  if (h2h.length === 0) return { winRate: null, sample: 0, history: [], mostRecentAt: null };

  const wins = h2h.filter(m => m.winner_id === teamAId).length;

  // Хронология: до 5 последних встреч с датой и тем, кто победил —
  // это даёт пользователю возможность увидеть "устарела" ли эта статистика
  // (например, единственная встреча была 2 года назад при другом составе).
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

/**
 * Сила турнира — контекст, а не сигнал "кто сильнее". Топ-турнир с крупным
 * призовым фондом обычно даёт более предсказуемую игру (команды выкладываются
 * полностью на LAN), чем закрытый онлайн-квалификатор низкого tier.
 */
function describeTournamentStrength(tournament) {
  const tier = tournament?.tier || null; // 's', 'a', 'b', 'c', 'd' по данным PandaScore
  const prizepoolRaw = tournament?.prizepool || null; // строка вида "10,000 United States Dollar"
  const type = tournament?.type || null; // 'online' | 'offline'

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

/**
 * Сводит форму, h2h, разницу раундов и momentum в единую вероятность победы
 * команды A. Веса:
 *  - 45% — текущая форма (винрейт по истории)
 *  - 20% — личные встречи (если есть достаточная и не слишком старая история)
 *  - 20% — средняя разница раундов, когда доступна (запас прочности, не только W/L)
 *  - 15% — momentum: набирает или теряет форму команда прямо сейчас
 * Без претензии на точность профессионального беттинг-алгоритма — это
 * ориентир на основе публичной статистики, не точный прогноз.
 */
function estimateProbability(formA, formB, h2h) {
  const formScore = 0.5 + (formA.winRate - formB.winRate) / 2;

  let roundWeight = 0.20;
  let roundScore = 0.5;
  if (formA.roundDataAvailable && formB.roundDataAvailable) {
    const roundDiffNorm = Math.tanh((formA.avgRoundDiff - formB.avgRoundDiff) / 8);
    roundScore = 0.5 + roundDiffNorm / 2;
  } else {
    roundWeight = 0; // нет данных — не подмешиваем фактор вообще
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
    const momentumDiff = formA.momentum - formB.momentum; // от -2 до 2
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

/**
 * Возвращает числовой score уверенности (0-100, для сортировки списка
 * матчей) и текстовую метку. Учитывает: размер выборки по обеим командам,
 * наличие и количество личных встреч, и насколько раздельны вероятности
 * (50/50 = низкая уверенность независимо от объёма данных, потому что
 * сам матч статистически близкий, а не потому что данных мало).
 */
function computeConfidence(sampleA, sampleB, h2hSample, probabilityGapPercent) {
  const minSample = Math.min(sampleA, sampleB);

  // Компонент 1: объём истории (0-50 баллов)
  const sampleScore = Math.min(minSample / 10, 1) * 50;

  // Компонент 2: личные встречи (0-20 баллов)
  const h2hScore = Math.min(h2hSample / 4, 1) * 20;

  // Компонент 3: насколько явный фаворит (0-30 баллов). probabilityGapPercent —
  // это |вероятность А - вероятность Б|, от 0 (полный паритет) до ~84 (92 vs 8).
  const gapScore = Math.min(probabilityGapPercent / 84, 1) * 30;

  const score = Math.round(sampleScore + h2hScore + gapScore);

  let label;
  if (score >= 70) label = 'высокая';
  else if (score >= 40) label = 'средняя';
  else label = 'низкая — мало данных или близкий матч';

  return { score, label };
}

/**
 * Полный анализ одного матча: форма команд, h2h, сила турнира, карты серии,
 * вероятность и уверенность. Throws при ошибках — вызывающий код решает,
 * как их обрабатывать (один матч → 500 с деталями, batch → пропустить
 * этот матч и продолжить остальные).
 */
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

/**
 * Один цикл обновления: тянет свежее расписание, считает аналитику для
 * первых MATCHES_TO_PRERENDER матчей, обновляет publicState целиком —
 * атомарно (посетитель никогда не увидит наполовину обновлённое состояние,
 * потому что переменная переписывается одним присваиванием в конце).
 */
async function refreshCycle() {
  if (publicState.isRefreshing) return; // защита от перекрытия циклов
  publicState.isRefreshing = true;

  try {
    const upcoming = await pandaFetch('/matches/upcoming', { per_page: 30, sort: 'begin_at' });

    const toAnalyze = upcoming.slice(0, MATCHES_TO_PRERENDER);
    const analyzed = [];

    for (const m of toAnalyze) {
      try {
        const result = await analyzeMatch(m.id);
        analyzed.push(result);
      } catch (e) {
        // Один неудачный матч (например TBD-команда) не должен обрушить
        // весь цикл обновления — пропускаем и продолжаем остальные.
        console.warn(`Пропущен матч ${m.id} при фоновом обновлении: ${e.message}`);
      }
    }

    analyzed.sort((a, b) => b.confidence.score - a.confidence.score);

    publicState.upcomingMatches = upcoming;
    publicState.analyzedMatches = analyzed;
    publicState.lastUpdatedAt = new Date().toISOString();
    publicState.lastError = null;
  } catch (e) {
    console.error('Ошибка фонового обновления:', e.message);
    publicState.lastError = e.message;
    // Не затираем предыдущие данные при ошибке — лучше показать
    // посетителям чуть устаревшие данные, чем пустой экран.
  } finally {
    publicState.isRefreshing = false;
  }
}

function startBackgroundRefresh() {
  refreshCycle(); // первый прогон сразу при старте сервера, не дожидаясь таймера
  setInterval(refreshCycle, REFRESH_INTERVAL_MS);
}

/* ===================== ЭНДПОИНТЫ (только чтение из publicState) ===================== */

app.get('/api/matches/upcoming', (req, res) => {
  if (!publicState.lastUpdatedAt) {
    return res.status(503).json({ error: 'Данные готовятся, обновите страницу через несколько секунд.' });
  }
  res.json(publicState.upcomingMatches);
});

app.get('/api/matches/analyzed', (req, res) => {
  if (!publicState.lastUpdatedAt) {
    return res.status(503).json({ error: 'Данные готовятся, обновите страницу через несколько секунд.' });
  }
  res.json({
    results: publicState.analyzedMatches,
    lastUpdatedAt: publicState.lastUpdatedAt,
    nextUpdateInMs: REFRESH_INTERVAL_MS,
  });
});

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
  });
});

app.get('/health', (req, res) => res.json({ ok: true }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`CS2 analytics backend running on port ${PORT}`);
  startBackgroundRefresh();
});
