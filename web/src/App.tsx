import { BrowserRouter, Routes, Route } from 'react-router-dom'

function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-background text-foreground">
        <Routes>
          <Route path="/" element={<div className="p-8 text-center"><h1 className="text-2xl font-bold">QRP Atlas</h1><p className="text-muted-foreground mt-2">交易复盘可视化平台</p></div>} />
        </Routes>
      </div>
    </BrowserRouter>
  )
}

export default App
