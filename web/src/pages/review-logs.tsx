import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'

export default function ReviewLogs() {
  return (
    <Card>
      <CardHeader>
        <CardTitle>复盘日志</CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-sm text-muted-foreground">
          按日期、股票、主题筛选的复盘记录将在此展示。
        </p>
      </CardContent>
    </Card>
  )
}
