import { useParams } from '@solidjs/router'
import RolloutDetail from './RolloutDetail'

export default function DeploymentDetail() {
  useParams()
  return <RolloutDetail />
}
