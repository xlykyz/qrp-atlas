import { useEffect, useState, type ReactNode } from 'react'
import { NavLink, Outlet } from 'react-router-dom'

const navItems = [
  { to: '/', label: '今日概览' },
  { to: '/stock', label: '个股复盘' },
  { to: '/logs', label: '复盘日志' },
]

function getInitialTheme(): boolean {
  const stored = localStorage.getItem('theme')
  if (stored === 'light') return false
  if (stored === 'dark') return true
  return window.matchMedia('(prefers-color-scheme: dark)').matches
}

function applyTheme(isDark: boolean) {
  const root = document.documentElement
  if (isDark) {
    root.classList.add('dark')
  } else {
    root.classList.remove('dark')
  }
  localStorage.setItem('theme', isDark ? 'dark' : 'light')
}

export default function Layout() {
  const [isDark, setIsDark] = useState(getInitialTheme)
  const [pageTitle, setPageTitle] = useState('')
  const [headerControls, setHeaderControls] = useState<ReactNode | null>(null)

  // initial apply
  useEffect(() => {
    applyTheme(isDark)
  }, [])

  // listen for system preference changes
  useEffect(() => {
    const mq = window.matchMedia('(prefers-color-scheme: dark)')
    const handler = (e: MediaQueryListEvent) => {
      // only change if no explicit user preference stored
      const stored = localStorage.getItem('theme')
      if (!stored) {
        setIsDark(e.matches)
        applyTheme(e.matches)
      }
    }
    mq.addEventListener('change', handler)
    return () => mq.removeEventListener('change', handler)
  }, [])

  function toggleTheme() {
    setIsDark(prev => {
      const next = !prev
      applyTheme(next)
      return next
    })
  }

  return (
    <div className="flex h-screen">
      {/* Left sidebar */}
      <aside className="flex w-64 flex-col bg-slate-900 dark:bg-slate-950 text-white">
        {/* Brand */}
        <div className="flex items-center gap-2 px-6 py-5">
          <span className="text-lg font-bold tracking-tight">QRP Atlas</span>
        </div>

        {/* Navigation */}
        <nav className="flex-1 space-y-1 px-3">
          {navItems.map(item => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.to === '/'}
              className={({ isActive }) =>
                `flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm font-medium transition-colors ${
                  isActive
                    ? 'bg-slate-800 text-white'
                    : 'text-slate-400 hover:bg-slate-800 hover:text-white'
                }`
              }
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
      </aside>

      {/* Right content area */}
      <main className="flex flex-1 flex-col bg-white text-slate-900 dark:bg-slate-950 dark:text-white">
        <div className="flex-1 overflow-auto">
          {/* Top bar with title, controls and theme toggle */}
          <div className="flex items-center gap-4 px-6 py-3 border-b border-slate-200 dark:border-slate-800">
            <h1 className="text-xl font-bold text-slate-900 dark:text-white flex-1">
              {pageTitle}
            </h1>
            {headerControls}
            <button
              onClick={toggleTheme}
              className="flex items-center gap-2 rounded-lg px-3 py-2 text-sm text-slate-500 dark:text-slate-400 transition-colors hover:bg-slate-100 dark:hover:bg-slate-800"
            >
              {isDark ? <span>☀️</span> : <span>🌙</span>}
              <span>{isDark ? '亮色模式' : '暗色模式'}</span>
            </button>
          </div>
          <div className="p-6">
            <Outlet context={{ setPageTitle, setHeaderControls }} />
          </div>
        </div>
      </main>
    </div>
  )
}
