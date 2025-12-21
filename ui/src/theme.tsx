import { createContext, useContext, useState, useEffect, type ReactNode } from 'react'

type Theme = 'light' | 'dark' | 'system'
type UIStyle = 'default' | 'superhuman' | 'gmail'

interface ThemeContextType {
  theme: Theme
  actualTheme: 'light' | 'dark'
  setTheme: (theme: Theme) => void
  uiStyle: UIStyle
  setUIStyle: (style: UIStyle) => void
}

const ThemeContext = createContext<ThemeContextType | undefined>(undefined)

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setTheme] = useState<Theme>(() => {
    const stored = localStorage.getItem('eml-theme')
    return (stored as Theme) || 'system'
  })

  const [uiStyle, setUIStyle] = useState<UIStyle>(() => {
    const stored = localStorage.getItem('eml-ui-style')
    return (stored as UIStyle) || 'default'
  })

  const [systemTheme, setSystemTheme] = useState<'light' | 'dark'>(() => {
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
  })

  const actualTheme = theme === 'system' ? systemTheme : theme

  useEffect(() => {
    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)')
    const handleChange = (e: MediaQueryListEvent) => {
      setSystemTheme(e.matches ? 'dark' : 'light')
    }
    mediaQuery.addEventListener('change', handleChange)
    return () => mediaQuery.removeEventListener('change', handleChange)
  }, [])

  useEffect(() => {
    localStorage.setItem('eml-theme', theme)
    document.documentElement.setAttribute('data-theme', actualTheme)
  }, [theme, actualTheme])

  useEffect(() => {
    localStorage.setItem('eml-ui-style', uiStyle)
    document.documentElement.setAttribute('data-ui-style', uiStyle)
  }, [uiStyle])

  return (
    <ThemeContext.Provider value={{ theme, actualTheme, setTheme, uiStyle, setUIStyle }}>
      {children}
    </ThemeContext.Provider>
  )
}

export function useTheme() {
  const context = useContext(ThemeContext)
  if (context === undefined) {
    throw new Error('useTheme must be used within a ThemeProvider')
  }
  return context
}
