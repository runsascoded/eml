import { useState } from 'react'
import { MdBrightnessAuto, MdLightMode, MdDarkMode } from 'react-icons/md'
import { useTheme } from '../theme'
import './ThemeToggle.scss'

export function ThemeToggle() {
  const { theme, setTheme } = useTheme()
  const [isHovering, setIsHovering] = useState(false)

  const cycleTheme = () => {
    if (theme === 'light') setTheme('dark')
    else if (theme === 'dark') setTheme('system')
    else setTheme('light')
  }

  const getThemeIcon = () => {
    switch (theme) {
      case 'light': return <MdLightMode />
      case 'dark': return <MdDarkMode />
      case 'system': return <MdBrightnessAuto />
    }
  }

  const getThemeLabel = () => {
    switch (theme) {
      case 'light': return 'Light'
      case 'dark': return 'Dark'
      case 'system': return 'System'
    }
  }

  return (
    <div
      className="theme-controls-container"
      onMouseEnter={() => setIsHovering(true)}
      onMouseLeave={() => setIsHovering(false)}
    >
      <div className={`theme-controls ${isHovering ? 'visible' : ''}`}>
        <button
          className="theme-toggle"
          onClick={cycleTheme}
          title={`Theme: ${getThemeLabel()}`}
          aria-label={`Current theme: ${getThemeLabel()}. Click to cycle themes.`}
        >
          <span className="theme-icon">{getThemeIcon()}</span>
        </button>
      </div>
    </div>
  )
}
