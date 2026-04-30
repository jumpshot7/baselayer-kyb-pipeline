'use client'

import { useEffect, useState } from 'react'
import Link from 'next/link'
import styles from './anomalies.module.css'

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8080'

const FLAGS = [
  { key: 'flag_predates', param: 'flag_predates', label: 'Predates Formation', color: 'var(--flag-predates)' },
  { key: 'flag_dormant', param: 'flag_dormant', label: 'Dormant Entity', color: 'var(--flag-dormant)' },
  { key: 'flag_address', param: 'flag_address', label: 'Address Mismatch', color: 'var(--flag-address)' },
]

export default function AnomaliesPage() {
  const [anomalies, setAnomalies] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [offset, setOffset] = useState(0)
  const [total, setTotal] = useState(0)
  const [activeFlag, setActiveFlag] = useState<string | null>(null)
  const LIMIT = 50

  async function fetchAnomalies(off = 0, flag: string | null = null) {
    setLoading(true)
    try {
      let url = `${API}/anomalies?has_anomaly=true&limit=${LIMIT}&offset=${off}`
      if (flag) url += `&${flag}=true`
      const data = await fetch(url).then(r => r.json())
      setAnomalies(data.results || [])
      setTotal(data.count || 0)
    } catch (e) {
      console.error(e)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchAnomalies(0, activeFlag)
  }, [])

  function handleFlag(flag: string | null) {
    setActiveFlag(flag)
    setOffset(0)
    fetchAnomalies(0, flag)
  }

  function handlePage(dir: 'prev' | 'next') {
    const newOffset = dir === 'next' ? offset + LIMIT : offset - LIMIT
    setOffset(newOffset)
    fetchAnomalies(newOffset, activeFlag)
  }

  return (
    <div className={styles.page}>
      <div className={styles.header}>
        <div>
          <p className={styles.eyebrow}>COMPLIANCE ANOMALIES</p>
          <h1 className={styles.title}>Flagged Entities</h1>
        </div>
        <div className={styles.filters}>
          <button
            className={`${styles.filter} ${activeFlag === null ? styles.filterActive : ''}`}
            onClick={() => handleFlag(null)}
          >
            All Flags
          </button>
          {FLAGS.map(f => (
            <button
              key={f.key}
              className={`${styles.filter} ${activeFlag === f.param ? styles.filterActive : ''}`}
              style={activeFlag === f.param ? { borderColor: f.color, color: f.color } : {}}
              onClick={() => handleFlag(f.param)}
            >
              {f.label}
            </button>
          ))}
        </div>
      </div>

      <div className={styles.tableWrap}>
        <table className={styles.table}>
          <thead>
            <tr>
              <th>NYC BUSINESS</th>
              <th>NYS ENTITY</th>
              <th>BOROUGH</th>
              <th>SCORE</th>
              <th>FLAGS</th>
              <th>STATUS</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr>
                <td colSpan={7} className={styles.loadingRow}>
                  <span className={styles.loadingDot} /> Loading...
                </td>
              </tr>
            ) : anomalies.length === 0 ? (
              <tr>
                <td colSpan={7} className={styles.emptyRow}>No anomalies found</td>
              </tr>
            ) : (
              anomalies.map(a => (
                <tr key={a.id} className={styles.row}>
                  <td>
                    <div className={styles.bizName}>{a.business_name}</div>
                    <div className={styles.bizSub}>{a.license_number}</div>
                  </td>
                  <td>
                    <div className={styles.bizName}>{a.current_entity_name}</div>
                    <div className={styles.bizSub}>{a.dos_id}</div>
                  </td>
                  <td>
                    <span className={styles.borough}>{a.borough || '—'}</span>
                  </td>
                  <td>
                    <span
                      className={styles.score}
                      style={{
                        color: parseFloat(a.match_score) === 100
                          ? 'var(--accent-gold)'
                          : 'var(--text-primary)'
                      }}
                    >
                      {parseFloat(a.match_score).toFixed(0)}
                    </span>
                  </td>
                  <td>
                    <div className={styles.flags}>
                      {a.flag_license_predates_formation && (
                        <span className={styles.flag} style={{ background: 'rgba(240,165,0,0.15)', color: 'var(--flag-predates)' }}>
                          PREDATES
                        </span>
                      )}
                      {a.flag_entity_dormant && (
                        <span className={styles.flag} style={{ background: 'rgba(232,93,4,0.15)', color: 'var(--flag-dormant)' }}>
                          DORMANT
                        </span>
                      )}
                      {a.flag_address_mismatch && (
                        <span className={styles.flag} style={{ background: 'rgba(59,130,246,0.15)', color: 'var(--flag-address)' }}>
                          ADDRESS
                        </span>
                      )}
                    </div>
                  </td>
                  <td>
                    <span
                      className={styles.status}
                      style={{
                        color: a.license_status === 'Active' ? 'var(--success)' : 'var(--text-muted)',
                        background: a.license_status === 'Active'
                          ? 'rgba(16,185,129,0.1)'
                          : 'rgba(255,255,255,0.04)'
                      }}
                    >
                      {a.license_status || '—'}
                    </span>
                  </td>
                  <td>
                    <Link href={`/anomalies/${a.id}`} className={styles.detailLink}>
                      →
                    </Link>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      <div className={styles.pagination}>
        <span className={styles.pageInfo}>
          Showing {offset + 1}–{Math.min(offset + LIMIT, offset + anomalies.length)} of {total} anomalies
        </span>
        <div className={styles.pageButtons}>
          <button
            className={styles.pageBtn}
            onClick={() => handlePage('prev')}
            disabled={offset === 0}
          >
            ← Prev
          </button>
          <button
            className={styles.pageBtn}
            onClick={() => handlePage('next')}
            disabled={anomalies.length < LIMIT}
          >
            Next →
          </button>
        </div>
      </div>
    </div>
  )
}
