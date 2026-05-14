import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'

export default function StockReview() {
  return (
    <Card>
      <CardHeader>
        <CardTitle>个股复盘</CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-sm text-muted-foreground">
          K 线图（含 MA/MA5/MA10/MA20）、复盘笔记、数据表将在此展示。
        </p>
      </CardContent>
    </Card>
  )
}
