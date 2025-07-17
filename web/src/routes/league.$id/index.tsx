import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/league/$id/')({
  component: RouteComponent,
})

function RouteComponent() {
  return <div>Hello "/league/$id/"!</div>
}
