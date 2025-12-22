import { Link, useLocation } from 'react-router-dom'
import './NotFound.scss'

export function NotFound() {
  const location = useLocation()

  return (
    <div className="not-found">
      <h1>404 - Page Not Found</h1>
      <p className="path">
        <code>{location.pathname}</code>
      </p>
      <p>The page you're looking for doesn't exist.</p>
      <nav className="links">
        <Link to="/">Go to Inbox</Link>
        <Link to="/admin">Go to Admin</Link>
      </nav>
    </div>
  )
}
