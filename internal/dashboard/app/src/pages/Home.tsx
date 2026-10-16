import { Flex, H1 } from '@traefik-labs/faency'

import PageTitle from '../layout/PageTitle'

const Home = () => {
  return (
    <Flex direction="column" gap={3}>
      <PageTitle title="Home" />
      <H1>GAP Dashboard</H1>
    </Flex>
  )
}

export default Home
