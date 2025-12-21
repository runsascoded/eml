import { useState } from 'react'
import { MdBrightnessAuto, MdLightMode, MdDarkMode, MdSettings } from 'react-icons/md'
import { useTheme } from '../theme'
import './ThemeToggle.scss'

type Theme = 'light' | 'dark' | 'system'
type UIStyle = 'default' | 'superhuman' | 'gmail'

export function ThemeToggle() {
  const { theme, setTheme, uiStyle, setUIStyle } = useTheme()
  const [isHovering, setIsHovering] = useState(false)

  const getThemeIcon = (t: Theme) => {
    switch (t) {
      case 'light': return <MdLightMode />
      case 'dark': return <MdDarkMode />
      case 'system': return <MdBrightnessAuto />
    }
  }

  const getThemeLabel = (t: Theme) => {
    switch (t) {
      case 'light': return 'Light'
      case 'dark': return 'Dark'
      case 'system': return 'System'
    }
  }

  const getStyleLabel = (s: UIStyle) => {
    switch (s) {
      case 'default': return 'Default'
      case 'superhuman': return 'Superhuman'
      case 'gmail': return 'Gmail'
    }
  }

  const themes: Theme[] = ['light', 'dark', 'system']
  const styles: UIStyle[] = ['default', 'superhuman', 'gmail']

  return (
    <div
      className="settings-container"
      onMouseEnter={() => setIsHovering(true)}
      onMouseLeave={() => setIsHovering(false)}
    >
      <div className={`settings-panel ${isHovering ? 'visible' : ''}`}>
        <div className="settings-section">
          <div className="section-label">Theme</div>
          <div className="option-buttons">
            {themes.map(t => (
              <button
                key={t}
                className={`option-btn ${theme === t ? 'active' : ''}`}
                onClick={() => setTheme(t)}
                title={getThemeLabel(t)}
              >
                <span className="option-icon">{getThemeIcon(t)}</span>
                <span className="option-label">{getThemeLabel(t)}</span>
              </button>
            ))}
          </div>
        </div>
        <div className="settings-section">
          <div className="section-label">Style</div>
          <div className="option-buttons">
            {styles.map(s => (
              <button
                key={s}
                className={`option-btn ${uiStyle === s ? 'active' : ''}`}
                onClick={() => setUIStyle(s)}
                title={getStyleLabel(s)}
              >
                <span className="option-label">{getStyleLabel(s)}</span>
              </button>
            ))}
          </div>
        </div>
      </div>
      <button className="settings-trigger" title="Settings">
        <MdSettings />
      </button>
    </div>
  )
}
