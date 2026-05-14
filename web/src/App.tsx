import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Layout from '@/components/layout'
import Overview from '@/pages/overview'
import StockReview from '@/pages/stock-review'
import ReviewLogs from '@/pages/review-logs'

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route path="/" element={<Overview />} />
          <Route path="/stock" element={<StockReview />} />
          <Route path="/logs" element={<ReviewLogs />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}

export default App
