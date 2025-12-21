import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { ThemeProvider } from './theme'
import { ThemeToggle } from './components/ThemeToggle'
import { Dashboard } from './components/Dashboard'
import { EmailViewer } from './components/EmailViewer'
import { EmailBrowser } from './components/EmailBrowser'
import { SyncRunDetail } from './components/SyncRunDetail'
import { SyncRunsPage } from './components/SyncRunsPage'
import { FolderDetail } from './components/FolderDetail'
import { ThreadViewer } from './components/ThreadViewer'
import { NotFound } from './components/NotFound'
import './styles/main.scss'

function App() {
  return (
    <ThemeProvider>
      <BrowserRouter>
        <Routes>
          {/* Main email browser routes */}
          <Route path="/" element={<EmailBrowser />} />
          <Route path="/folder/*" element={<EmailBrowser />} />
          <Route path="/email/*" element={<EmailViewer />} />
          <Route path="/thread/*" element={<ThreadViewer />} />
          {/* Admin/sync routes */}
          <Route path="/admin" element={<Dashboard />} />
          <Route path="/admin/syncs" element={<SyncRunsPage />} />
          <Route path="/admin/folder/:folder" element={<FolderDetail />} />
          <Route path="/admin/sync/:runId" element={<SyncRunDetail />} />
          {/* 404 catch-all */}
          <Route path="*" element={<NotFound />} />
        </Routes>
        <ThemeToggle />
      </BrowserRouter>
    </ThemeProvider>
  )
}

export default App
