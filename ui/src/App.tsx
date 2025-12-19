import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { ThemeProvider } from './theme'
import { ThemeToggle } from './components/ThemeToggle'
import { Dashboard } from './components/Dashboard'
import { EmailViewer } from './components/EmailViewer'
import { EmailBrowser } from './components/EmailBrowser'
import { SyncRunDetail } from './components/SyncRunDetail'
import { FolderDetail } from './components/FolderDetail'
import './styles/main.scss'

function App() {
  return (
    <ThemeProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/browse" element={<EmailBrowser />} />
          <Route path="/browse/:account/:folder" element={<EmailBrowser />} />
          <Route path="/folder/:account/:folder" element={<FolderDetail />} />
          <Route path="/email/*" element={<EmailViewer />} />
          <Route path="/sync/:runId" element={<SyncRunDetail />} />
        </Routes>
        <ThemeToggle />
      </BrowserRouter>
    </ThemeProvider>
  )
}

export default App
