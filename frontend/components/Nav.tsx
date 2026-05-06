'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import styles from './Nav.module.css'

const links = [
  { href: '/', label: 'Dashboard' },
  { href: '/anomalies', label: 'Anomalies' },
  { href: '/search', label: 'Search' },
]

export default function Nav() {
  const pathname = usePathname()

  return (
    <nav className={styles.nav}>
      <div className={styles.brand}>
        <img
          className={styles.brandMark}
          src="/favicon.ico"
          alt="KYB logo"
        />
        <span className={styles.brandName}>KYB<span className={styles.brandSub}>COMPLIANCE ENGINE</span></span>
      </div>

      <div className={styles.links}>
        {links.map(link => (
          <Link
            key={link.href}
            href={link.href}
            className={`${styles.link} ${pathname === link.href ? styles.active : ''}`}
          >
            {link.label}
          </Link>
        ))}
      </div>

      <div className={styles.badge}>
        <span className={styles.dot} />
        LIVE
      </div>
    </nav>
  )
}
