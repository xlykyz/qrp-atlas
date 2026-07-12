import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Layout from '@/components/layout'
import Overview from '@/pages/overview'
import StockReview from '@/pages/stock-review'
import ReviewLogs from '@/pages/review-logs'
import RawPreview from '@/pages/raw-preview'
import BacktestAnalysis from '@/pages/backtest-analysis'
import BacktestWorkflowPage from '@/pages/backtest-workflow'

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route path="/" element={<Overview />} />
          <Route path="/stock" element={<StockReview />} />
          <Route path="/logs" element={<ReviewLogs />} />
          <Route path="/raw" element={<RawPreview />} />
          <Route path="/backtest" element={<BacktestAnalysis />} />
          <Route path="/backtest/workflow" element={<BacktestWorkflowPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}

export default App
