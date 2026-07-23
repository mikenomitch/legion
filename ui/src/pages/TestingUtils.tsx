import { Card, EmptyState } from '../components/ui'

export default function TestingUtils() {
  return (
    <Card pad={false}>
      <EmptyState
        icon="boxes"
        title="Testing utilities coming soon"
        body="Chaos tooling and cluster test helpers will live here."
      />
    </Card>
  )
}
