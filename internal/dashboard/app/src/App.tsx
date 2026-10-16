import { darkTheme, FaencyProvider, lightTheme } from '@traefik-labs/faency'
import { useEffect } from 'react'
import { HelmetProvider } from 'react-helmet-async'
import { BrowserRouter, Route, Routes, useLocation } from 'react-router-dom'

import Page from './layout/Page'
import { routes } from './config/routes'
import { useIsDarkMode } from './hooks/use-theme'

export const LIGHT_THEME = lightTheme('blue')
export const DARK_THEME = darkTheme('blue')

const ScrollToTop = () => {
  const { pathname } = useLocation()
  useEffect(() => {
    window.scrollTo(0, 0)
  }, [pathname])

  return null
}

const AppContent = () => {
  return (
    <Page>
      <ScrollToTop />
      <Routes>
        {routes.map((route) => (
          <Route key={route.path} path={route.path} element={<route.component />} />
        ))}
      </Routes>
    </Page>
  )
}

const getBase = () => {
  switch (import.meta.env.MODE) {
    case 'development':
      return ''
    default:
  }

  const base = document.querySelector('base')
  if (base && base.href) {
    const url = new URL(base.href)
    return url.pathname.replace(/\/$/, '')
  }
  return ''
}

const App = () => {
  const isDarkMode = useIsDarkMode()

  useEffect(() => {
    if (isDarkMode) {
      document.documentElement.classList.remove(LIGHT_THEME)
      document.documentElement.classList.add(DARK_THEME)
    } else {
      document.documentElement.classList.remove(DARK_THEME)
      document.documentElement.classList.add(LIGHT_THEME)
    }
  }, [isDarkMode])

  return (
    <FaencyProvider>
      <HelmetProvider>
        <BrowserRouter basename={getBase()}>
          <AppContent />
        </BrowserRouter>
      </HelmetProvider>
    </FaencyProvider>
  )
}

export default App
