'use client'

import { useEffect, useState } from 'react'
import { useParams } from 'next/navigation'
import Link from 'next/link'
import styles from './detail.module.css'

const API = '/api'

function FlagRow({ active, color, bg, label, description }: any) {
  return (
    <div
      className={styles.flagRow}
      style={active ? { background: bg, borderColor: color } : {}}
    >
      <div className={styles.flagIndicator} style={{ background: active ? color : 'var(--border)' }} />
      <div className={styles.flagInfo}>
        <span className={styles.flagLabel} style={active ? { color } : {}}>
          {active ? '▲ ' : ''}{label}
        </span>
        <span className={styles.flagDesc}>{description}</span>
      </div>
      <span
        className={styles.flagStatus}
        style={{
          color: active ? color : 'var(--text-muted)',
          background: active ? bg : 'transparent',
        }}
      >
        {active ? 'FLAGGED' : 'CLEAR'}
      </span>
    </div>
  )
}

export default function AnomalyDetailPage() {
  const { id } = useParams()
  const [data, setData] = useState<any>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetch(`${API}/anomalies/${id}`)
      .then(r => r.json())
      .then(setData)
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [id])

  if (loading) return (
    <div className={styles.loading}>
      <div className={styles.loadingDot} />
      <span>LOADING RECORD</span>
    </div>
  )

  if (!data) return (
    <div className={styles.loading}>
      <span>Anomaly not found</span>
    </div>
  )

  const nyc = data.nyc_business || {}
  const nys = data.nys_entity || {}
  const hasAny = data.has_anomaly

  return (
    <div className={styles.page}>
      <div className={styles.back}>
        <Link href="/anomalies" className={styles.backLink}>
          ← Back to Anomalies
        </Link>
        <span className={styles.id}>RECORD #{id}</span>
      </div>

      {/* Header */}
      <div className={styles.header}>
        <div className={styles.headerLeft}>
          <p className={styles.eyebrow}>ANOMALY DETAIL</p>
          <h1 className={styles.title}>{nyc.business_name}</h1>
          <p className={styles.subtitle}>Matched against → {nys.current_entity_name}</p>
        </div>
        <div className={styles.scoreBox}>
          <span className={styles.scoreLabel}>MATCH SCORE</span>
          <span
            className={styles.scoreValue}
            style={{ color: parseFloat(data.match_score) === 100 ? 'var(--accent-gold)' : 'var(--text-primary)' }}
          >
            {parseFloat(data.match_score).toFixed(0)}
          </span>
          <span className={styles.scoreMax}>/100</span>
        </div>
      </div>

      {/* Verdict */}
      <div
        className={styles.verdict}
        style={{
          background: hasAny ? 'rgba(240,165,0,0.06)' : 'rgba(16,185,129,0.06)',
          borderColor: hasAny ? 'rgba(240,165,0,0.3)' : 'rgba(16,185,129,0.3)',
        }}
      >
        <span style={{ color: hasAny ? 'var(--accent-gold)' : 'var(--success)', fontSize: 20 }}>
          {hasAny ? '⚠' : '✓'}
        </span>
        <div>
          <p className={styles.verdictTitle} style={{ color: hasAny ? 'var(--accent-gold)' : 'var(--success)' }}>
            {hasAny ? 'ANOMALIES DETECTED' : 'NO ANOMALIES DETECTED'}
          </p>
          <p className={styles.verdictDesc}>
            {hasAny
              ? 'This matched pair has triggered one or more compliance flags requiring review.'
              : 'This matched pair appears compliant across all checked signals.'
            }
          </p>
        </div>
      </div>

      {/* Two column layout */}
      <div className={styles.grid}>

        {/* NYC DCA Business */}
        <div className={styles.card}>
          <div className={styles.cardHeader}>
            <span className={styles.cardTag}>NYC DCA</span>
            <h2 className={styles.cardTitle}>License Record</h2>
          </div>
          <div className={styles.fields}>
            <Field label="Business Name" value={nyc.business_name} />
            <Field label="License Number" value={nyc.license_number} mono />
            <Field label="License Type" value={nyc.license_type} />
            <Field
              label="License Status"
              value={nyc.license_status}
              highlight={
                nyc.license_status === 'Active' ? 'success' :
                nyc.license_status === 'Expired' || nyc.license_status === 'Surrendered' ? 'danger' : undefined
              }
            />
            <Field label="Initial Issuance Date" value={nyc.initial_issuance_date} mono />
            <Field label="Expiration Date" value={nyc.expiration_date} mono />
            <Field label="Borough" value={nyc.borough} />
            <Field label="ZIP Code" value={nyc.zip_code} mono />
            <Field label="Business Category" value={nyc.business_category} />
          </div>
        </div>

        {/* NYS Entity */}
        <div className={styles.card}>
          <div className={styles.cardHeader}>
            <span className={styles.cardTag}>NYS DOS</span>
            <h2 className={styles.cardTitle}>Corporate Entity</h2>
          </div>
          <div className={styles.fields}>
            <Field label="Entity Name" value={nys.current_entity_name} />
            <Field label="DOS ID" value={nys.dos_id} mono />
            <Field label="Entity Type" value={nys.entity_type} />
            <Field label="DOS Process Name" value={nys.dos_process_name} />
            <Field label="Date of Formation" value={nys.initial_dos_filing_date || nys.date_of_formation} mono />
            <Field label="County" value={nys.county} />
            <Field label="ZIP Code" value={nys.zip_code} mono />
            <Field label="Jurisdiction" value={nys.jurisdiction} />
          </div>
        </div>

      </div>

      {/* Flags */}
      <div className={styles.flagSection}>
        <h2 className={styles.flagSectionTitle}>ANOMALY FLAGS</h2>
        <div className={styles.flagList}>
          <FlagRow
            active={data.flag_license_predates_formation}
            color="var(--flag-predates)"
            bg="rgba(240,165,0,0.08)"
            label="License Predates Formation"
            description="NYC license was issued before the NYS entity was formed. Signal: how was a license granted for an entity that didn't exist yet?"
          />
          <FlagRow
            active={data.flag_entity_dormant}
            color="var(--flag-dormant)"
            bg="rgba(232,93,4,0.08)"
            label="Entity Dormant"
            description="License is Expired or Surrendered but the NYS entity is still active and was formed more than 3 years ago. Business stopped operating but never formally dissolved."
          />
          <FlagRow
            active={data.flag_address_mismatch}
            color="var(--flag-address)"
            bg="rgba(59,130,246,0.08)"
            label="Address Mismatch"
            description="ZIP code on the NYC license doesn't match the NYS registered address. Entity may be operating from an unregistered location."
          />
          <FlagRow
            active={data.flag_license_active_entity_dissolved}
            color="var(--flag-dissolved)"
            bg="rgba(155,93,229,0.08)"
            label="Active License / Dissolved Entity"
            description="NYC license is active but the NYS entity is dissolved. Note: requires a dissolved corporations dataset — currently unavailable."
          />
        </div>
      </div>

    </div>
  )
}

function Field({ label, value, mono, highlight }: {
  label: string
  value?: string
  mono?: boolean
  highlight?: 'success' | 'danger'
}) {
  const color = highlight === 'success' ? 'var(--success)'
    : highlight === 'danger' ? 'var(--danger)'
    : 'var(--text-primary)'

  return (
    <div className={styles.field}>
      <span className={styles.fieldLabel}>{label}</span>
      <span
        className={`${styles.fieldValue} ${mono ? styles.fieldMono : ''}`}
        style={{ color }}
      >
        {value || '—'}
      </span>
    </div>
  )
}
