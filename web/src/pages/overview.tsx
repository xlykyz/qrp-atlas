import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'

export default function Overview() {
  return (
    <Card>
      <CardHeader>
        <CardTitle>今日概览</CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-sm text-muted-foreground">
          市场 KPI、涨停池、跌停池等核心数据将在此展示。
        </p>
      </CardContent>
    </Card>
  )
}
