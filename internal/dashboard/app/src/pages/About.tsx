import { Badge, Card, Flex, H1, Link, Paragraph, Text } from '@traefik-labs/faency'
import { FiGithub } from 'react-icons/fi'

import { useMetas } from '../hooks/useMetas'
import PageTitle from '../layout/PageTitle'

const description = { maxWidth: '42rem', fontSize: '$4', lineHeight: 1.65 }

const About = () => {
  const { metas, loading, error } = useMetas()

  return (
    <Flex direction="column" gap={4}>
      <PageTitle title="About" />
      <H1>About GAP Dashboard</H1>
      <Paragraph css={description}>
        GAP Dashboard is the operator console for this node. It presents messages published from the node, messages
        received by it, and the plugins that carry out that work.
      </Paragraph>
      <Text css={description}>Powered by Loopopen Inc.</Text>
      <Flex align="center" gap={2}>
        <H1 css={{ fontSize: '$6' }}>About GAP</H1>
        <Link
          href="https://github.com/loopopen/gap"
          target="_blank"
          aria-label="GAP on GitHub"
          css={{ color: '$purple11' }}
        >
          <FiGithub size={18} />
        </Link>
      </Flex>
      <Paragraph css={description}>
        GAP is a lightweight, event-driven messaging library for Go. It uses the outbox pattern, so a message is written
        in the same database transaction as your data, then delivered once that transaction commits. It speaks RabbitMQ
        and Kafka today, and it can store messages in MySQL or in any database GORM already supports. More brokers and
        databases are part of the plan. The library is shaped so those can be added without tearing up what you already
        have.
      </Paragraph>
      {error && <Text css={{ color: '$red10' }}>{error}</Text>}
      <Card css={{ p: 0, maxWidth: '50%' }}>
        {loading && !error && <Text css={{ p: '$3' }}>Loading…</Text>}
        {!loading && !error && metas.length === 0 && <Text css={{ p: '$3', color: '$textSubtle' }}>No metas found</Text>}
        {metas.map((item) => (
          <Flex
            key={`${item.type}-${item.plugin}-${item.version}`}
            justify="space-between"
            align="center"
            css={{ p: '$3', borderBottom: '1px solid $colors$tableRowBorder' }}
          >
            {item.type === 'Self' ? (
              <Badge variant="blue">GAP</Badge>
            ) : (
              <Badge variant="green">{`${item.type} / ${item.plugin}`}</Badge>
            )}
            <Text css={{ color: '$textSubtle' }}>{`version: ${item.version}`}</Text>
          </Flex>
        ))}
      </Card>
    </Flex>
  )
}

export default About
