'use client'

import { useState } from 'react'
import Link from 'next/link'
import styles from './search.module.css'

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8080'

export default function SearchPage() {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState<any[]>([])
  const [loading, setLoading] = useState(false)
  const [searched, setSearched] = useState(false)

  async function handleSearch() {
    if (query.trim().length < 2) return
    setLoading(true)
    setSearched(true)
    try {
      const data = await fetch(
        `${API}/businesses/search?name=${encodeURIComponent(query.trim())}&limit=30`
      ).then(r => r.json())
      setResults(data.results || [])
    } catch (e) {
      console.error(e)
    } finally {
      setLoading(false)
    }
  }

  function handleKey(e: React.KeyboardEvent) {
    if (e.key === 'Enter') handleSearch()
  }

  return (
    <div className={styles.page}>
      <div className={styles.header}>
        <p className={styles.eyebrow}>BUSINESS LOOKUP</p>
        <h1 className={styles.title}>Search NYC Businesses</h1>
        <p className={styles.subtitle}>
          Search the NYC DCA license database and view associated compliance anomalies.
        </p>
      </div>

      <div className={styles.searchBox}>
        <div className={styles.inputWrap}>
          <span className={styles.searchIcon}>⌕</span>
          <input
            className={styles.input}
            type="text"
            placeholder="Search by business name..."
            value={query}
            onChange={e => setQuery(e.target.value)}
            onKeyDown={handleKey}
            autoFocus
          />
        </div>
        <button
          className={styles.searchBtn}
          onClick={handleSearch}
          disabled={query.trim().length < 2 || loading}
        >
          {loading ? 'SEARCHING...' : 'SEARCH'}
        </button>
      </div>

      {!searched && (
        <div className={styles.empty}>
          <div className={styles.emptyIcon}>◈</div>
          <p>Enter a business name to begin searching</p>
          <p className={styles.emptyHint}>Minimum 2 characters · Case insensitive</p>
        </div>
      )}

      {searched && !loading && results.length === 0 && (
        <div className={styles.empty}>
          <div className={styles.emptyIcon}>∅</div>
          <p>No businesses found matching "{query}"</p>
        </div>
      )}

      {results.length > 0 && (
        <div className={styles.results}>
          <p className={styles.resultCount}>
            {results.length} result{results.length !== 1 ? 's' : ''} for "{query}"
          </p>
          <div className={styles.resultList}>
            {results.map(biz => (
              <BusinessCard key={biz.id} biz={biz} />
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function BusinessCard({ biz }: { biz: any }) {
  const [anomalies, setAnomalies] = useState<any[] | null>(null)
  const [expanded, setExpanded] = useState(false)
  const [loading, setLoading] = useState(false)

  async function toggle() {
    if (!expanded && anomalies === null) {
      setLoading(true)
      try {
        const data = await fetch(
          `${API}/businesses/${biz.license_number}`
        ).then(r => r.json())
        setAnomalies(data.anomalies || [])
      } catch (e) {
        setAnomalies([])
      } finally {
        setLoading(false)
      }
    }
    setExpanded(!expanded)
  }

  const statusColor = biz.license_status === 'Active'
    ? 'var(--success)'
    : biz.license_status === 'Expired' || biz.license_status === 'Surrendered'
    ? 'var(--danger)'
    : 'var(--text-muted)'

  return (
    <div className={styles.bizCard}>
      <div className={styles.bizCardTop} onClick={toggle}>
        <div className={styles.bizInfo}>
          <div className={styles.bizName}>{biz.business_name}</div>
          {biz.dba_trade_name && (
            <div className={styles.bizDba}>DBA: {biz.dba_trade_name}</div>
          )}
          <div className={styles.bizMeta}>
            <span className={styles.bizMetaItem}>{biz.license_number}</span>
            <span className={styles.bizMetaDot}>·</span>
            <span className={styles.bizMetaItem}>{biz.borough || '—'}</span>
            <span className={styles.bizMetaDot}>·</span>
            <span className={styles.bizMetaItem}>{biz.business_category || '—'}</span>
          </div>
        </div>
        <div className={styles.bizRight}>
          <span
            className={styles.bizStatus}
            style={{ color: statusColor, borderColor: statusColor }}
          >
            {biz.license_status || '—'}
          </span>
          <span className={styles.expand}>{expanded ? '▲' : '▼'}</span>
        </div>
      </div>

      {expanded && (
        <div className={styles.bizExpanded}>
          {loading ? (
            <p className={styles.loadingText}>Loading anomalies...</p>
          ) : anomalies && anomalies.length > 0 ? (
            <div className={styles.anomalyList}>
              <p className={styles.anomalyHeader}>
                {anomalies.length} anomaly match{anomalies.length !== 1 ? 'es' : ''} found
              </p>
              {anomalies.map((a, i) => (
                <div key={i} className={styles.anomalyRow}>
                  <div>
                    <div className={styles.anomalyEntity}>{a.current_entity_name}</div>
                    <div className={styles.anomalyFlags}>
                      {a.flag_license_predates_formation && (
                        <span className={styles.aFlag} style={{ color: 'var(--flag-predates)' }}>PREDATES</span>
                      )}
                      {a.flag_entity_dormant && (
                        <span className={styles.aFlag} style={{ color: 'var(--flag-dormant)' }}>DORMANT</span>
                      )}
                      {a.flag_address_mismatch && (
                        <span className={styles.aFlag} style={{ color: 'var(--flag-address)' }}>ADDRESS</span>
                      )}
                      {!a.has_anomaly && (
                        <span className={styles.aFlag} style={{ color: 'var(--text-muted)' }}>CLEAN MATCH</span>
                      )}
                    </div>
                  </div>
                  <span className={styles.anomalyScore}>
                    {parseFloat(a.match_score).toFixed(0)}
                  </span>
                </div>
              ))}
            </div>
          ) : (
            <p className={styles.noAnomalies}>✓ No anomalies found for this business</p>
          )}
        </div>
      )}
    </div>
  )
}
