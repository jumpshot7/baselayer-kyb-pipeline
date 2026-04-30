'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import styles from './page.module.css'

const API = '/api'

const BOROUGHS = ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island']

const FLAG_CONFIG = [
  {
    key: 'flag_license_predates_formation',
    label: 'License Predates Formation',
    description: 'License issued before entity existed',
    color: 'var(--flag-predates)',
    bg: 'rgba(196, 150, 42, 0.08)',
    border: 'rgba(196, 150, 42, 0.25)',
    icon: '⟲',
  },
  {
    key: 'flag_entity_dormant',
    label: 'Entity Dormant',
    description: 'Dead license, active legal shell',
    color: 'var(--flag-dormant)',
    bg: 'rgba(196, 98, 42, 0.08)',
    border: 'rgba(196, 98, 42, 0.25)',
    icon: '◎',
  },
  {
    key: 'flag_address_mismatch',
    label: 'Address Mismatch',
    description: 'Operating from unregistered location',
    color: 'var(--flag-address)',
    bg: 'rgba(61, 138, 90, 0.08)',
    border: 'rgba(61, 138, 90, 0.25)',
    icon: '⊘',
  },
]

function useCountUp(target: number, duration = 1500, start = false) {
  const [count, setCount] = useState(0)
  useEffect(() => {
    if (!start || target === 0) return
    let startTime: number
    let rafId: number
    const animate = (timestamp: number) => {
      if (!startTime) startTime = timestamp
      const progress = Math.min((timestamp - startTime) / duration, 1)
      const eased = 1 - Math.pow(1 - progress, 3)
      setCount(Math.floor(eased * target))
      if (progress < 1) rafId = requestAnimationFrame(animate)
      else setCount(target)
    }
    rafId = requestAnimationFrame(animate)
    return () => cancelAnimationFrame(rafId)
  }, [target, start, duration])
  return count
}

export default function DashboardPage() {
  const [summary, setSummary] = useState<any>(null)
  const [boroughData, setBoroughData] = useState<any[]>([])
  const [scoreData, setScoreData] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [animate, setAnimate] = useState(false)
  const [showBars, setShowBars] = useState(false)

  useEffect(() => {
    async function load() {
      try {
        const s = await fetch(`${API}/anomalies/summary`).then(r => r.json())
        setSummary(s)

        const boroughResults = await Promise.all(
          BOROUGHS.map(async b => {
            const r = await fetch(`${API}/anomalies/by-borough/${b}?limit=1`).then(r => r.json())
            return { borough: b, count: r.count || 0 }
          })
        )
        setBoroughData(boroughResults)

        const [chunk1, chunk2, chunk3] = await Promise.all([
          fetch(`${API}/anomalies?limit=500&offset=0`).then(r => r.json()),
          fetch(`${API}/anomalies?limit=500&offset=5000`).then(r => r.json()),
          fetch(`${API}/anomalies?limit=500&offset=10000`).then(r => r.json()),
        ])
        const allAnomalies = [
          ...(chunk1.results || []),
          ...(chunk2.results || []),
          ...(chunk3.results || []),
        ]
        const buckets: Record<string, number> = {
          '85-89': 0, '90-94': 0, '95-99': 0, '100': 0
        }
        allAnomalies.forEach((a: any) => {
          const s = parseFloat(a.match_score)
          if (s === 100) buckets['100']++
          else if (s >= 95) buckets['95-99']++
          else if (s >= 90) buckets['90-94']++
          else buckets['85-89']++
        })
        setScoreData(Object.entries(buckets).map(([range, count]) => ({ range, count })))
      } catch (e) {
        console.error(e)
      } finally {
        setLoading(false)
        setTimeout(() => setAnimate(true), 100)
        setTimeout(() => setShowBars(true), 300)
      }
    }
    load()
  }, [])

  const totalCount = useCountUp(summary?.total_anomalies || 0, 2000, animate)
  const predatesCount = useCountUp(summary?.flag_license_predates_formation || 0, 1800, animate)
  const dormantCount = useCountUp(summary?.flag_entity_dormant || 0, 1800, animate)
  const addressCount = useCountUp(summary?.flag_address_mismatch || 0, 1800, animate)

  const countMap: Record<string, number> = {
    flag_license_predates_formation: predatesCount,
    flag_entity_dormant: dormantCount,
    flag_address_mismatch: addressCount,
  }

  const maxBorough = Math.max(...boroughData.map(b => b.count), 1)
  const maxScore = Math.max(...scoreData.map(s => s.count), 1)

  if (loading) return (
    <div className={styles.loading}>
      <div className={styles.loadingDot} />
      <span>LOADING INTELLIGENCE</span>
    </div>
  )

  return (
    <div className={styles.page}>

      {/* Hero */}
      <div className={styles.hero}>
        <div className={styles.heroLeft}>
          <p className={styles.heroEyebrow}>KYB COMPLIANCE ENGINE</p>
          <h1 className={styles.heroTitle}>
            <span className={styles.heroNumber}>{totalCount.toLocaleString()}</span>
            <span className={styles.heroLabel}>Anomalies<br />Detected</span>
          </h1>
          <p className={styles.heroSub}>
            Fuzzy-matched NYC DCA licenses against NYS Active Corporations.
            Flagging predated licenses, dormant entities, and address discrepancies.
          </p>
          <Link href="/anomalies" className={styles.heroCta}>
            Investigate Anomalies →
          </Link>
        </div>

        <div className={styles.heroRight}>
          <div className={styles.statGrid}>
            <div className={styles.statBox}>
              <span className={styles.statValue}>69K+</span>
              <span className={styles.statLabel}>NYC DCA Licenses</span>
            </div>
            <div className={styles.statBox}>
              <span className={styles.statValue}>2M+</span>
              <span className={styles.statLabel}>NYS Entities</span>
            </div>
            <div className={styles.statBox}>
              <span className={styles.statValue}>85%</span>
              <span className={styles.statLabel}>Match Threshold</span>
            </div>
            <div className={styles.statBox}>
              <span className={styles.statValue}>4</span>
              <span className={styles.statLabel}>Anomaly Flags</span>
            </div>
          </div>
        </div>
      </div>

      {/* Flag Cards */}
      <div className={styles.section}>
        <h2 className={styles.sectionTitle}>ANOMALY FLAGS</h2>
        <div className={styles.flagGrid}>
          {FLAG_CONFIG.map(flag => (
            <div
              key={flag.key}
              className={styles.flagCard}
              style={{ background: flag.bg, borderColor: flag.border }}
            >
              <div className={styles.flagIcon} style={{ color: flag.color }}>
                {flag.icon}
              </div>
              <div className={styles.flagCount} style={{ color: flag.color }}>
                {(countMap[flag.key] || 0).toLocaleString()}
              </div>
              <div className={styles.flagLabel}>{flag.label}</div>
              <div className={styles.flagDesc}>{flag.description}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Charts Row */}
      <div className={styles.chartsRow}>

        {/* Borough Chart */}
        <div className={styles.chartCard}>
          <h2 className={styles.chartTitle}>ANOMALIES BY BOROUGH</h2>
          <div className={styles.barChart}>
            {boroughData.map(b => (
              <div key={b.borough} className={styles.barRow}>
                <span className={styles.barLabel}>{b.borough}</span>
                <div className={styles.barTrack}>
                  <div
                    className={styles.barFill}
                    style={{ width: showBars ? `${(b.count / maxBorough) * 100}%` : '0%' }}
                  />
                </div>
                <span className={styles.barValue}>{b.count.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Match Score Distribution */}
        <div className={styles.chartCard}>
          <h2 className={styles.chartTitle}>MATCH SCORE DISTRIBUTION</h2>
          <div className={styles.scoreChart}>
            {scoreData.map(s => (
              <div key={s.range} className={styles.scoreCol}>
                <span className={styles.scoreValue}>{s.count}</span>
                <div className={styles.scoreTrack}>
                  <div
                    className={styles.scoreFill}
                    style={{ height: showBars ? `${(s.count / maxScore) * 100}%` : '0%' }}
                  />
                </div>
                <span className={styles.scoreLabel}>{s.range}</span>
              </div>
            ))}
          </div>
          <p className={styles.scoreNote}>
            Score range 85–100 · token_sort_ratio matching
          </p>
        </div>

      </div>

      {/* Pipeline Info */}
      <div className={styles.pipeline}>
        <h2 className={styles.sectionTitle}>PIPELINE ARCHITECTURE</h2>
        <div className={styles.pipelineSteps}>
          {[
            { step: '01', label: 'INGEST', desc: 'Socrata API → Google Cloud Storage' },
            { step: '02', label: 'VALIDATE', desc: 'Apache Beam + Pydantic models' },
            { step: '03', label: 'LOAD', desc: 'Batch INSERT → Postgres via psycopg2' },
            { step: '04', label: 'MATCH', desc: 'RapidFuzz token_sort_ratio ≥ 85' },
            { step: '05', label: 'FLAG', desc: 'Anomaly detection across 4 signals' },
          ].map((s, i) => (
            <div key={s.step} className={styles.pipelineStep}>
              <span className={styles.pipelineNum}>{s.step}</span>
              <span className={styles.pipelineLabel}>{s.label}</span>
              <span className={styles.pipelineDesc}>{s.desc}</span>
              {i < 4 && <span className={styles.pipelineArrow}>→</span>}
            </div>
          ))}
        </div>
      </div>

    </div>
  )
}