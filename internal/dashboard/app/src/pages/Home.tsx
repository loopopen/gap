import { Badge, Box, Button, Card, Flex, H1, Text } from '@traefik-labs/faency'
import { useEffect, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  FiActivity,
  FiAlertTriangle,
  FiCheckCircle,
  FiClock,
  FiDatabase,
  FiDownload,
  FiRepeat,
  FiSend,
} from 'react-icons/fi'

import { useDashboardStats } from '../hooks/useDashboardStats'
import { useMetas } from '../hooks/useMetas'
import PageTitle from '../layout/PageTitle'
import type { MessageStats } from '../types'

const PROGRESS_RADIUS = 6.25
const PROGRESS_CIRCUMFERENCE = 2 * Math.PI * PROGRESS_RADIUS

const RefreshProgress = ({ startedAt, intervalMs }: { startedAt: number; intervalMs: number }) => {
  const arcRef = useRef<SVGCircleElement>(null)

  useEffect(() => {
    const arc = arcRef.current
    if (!arc || startedAt === 0) return
    const animation = arc.animate(
      [{ strokeDashoffset: PROGRESS_CIRCUMFERENCE }, { strokeDashoffset: 0 }],
      { duration: intervalMs, easing: 'linear', fill: 'forwards' },
    )
    return () => animation.cancel()
  }, [startedAt, intervalMs])

  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <circle cx="8" cy="8" r={PROGRESS_RADIUS} stroke="currentColor" strokeOpacity="0.25" strokeWidth="2" />
      <circle
        ref={arcRef}
        cx="8"
        cy="8"
        r={PROGRESS_RADIUS}
        stroke="currentColor"
        strokeWidth="2"
        strokeDasharray={PROGRESS_CIRCUMFERENCE}
        strokeDashoffset={PROGRESS_CIRCUMFERENCE}
        transform="rotate(-90 8 8)"
      />
    </svg>
  )
}

const numberFormatter = new Intl.NumberFormat()
const formatNumber = (value: number) => numberFormatter.format(value)

const formatDate = (value: string | null) => {
  if (!value) return 'No activity yet'
  return new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'short' }).format(new Date(value))
}

const successRate = (stats: MessageStats) => {
  if (!stats.total) return 0
  return Math.round((stats.succeeded / stats.total) * 100)
}

type MetricCardProps = {
  label: string
  value: number
  hint: string
  tone: 'blue' | 'green' | 'orange' | 'red'
  icon: React.ReactNode
}

const MetricCard = ({ label, value, hint, tone, icon }: MetricCardProps) => (
  <Card css={{ p: '$4', minWidth: 0 }}>
    <Flex justify="space-between" align="start" gap={3}>
      <Flex direction="column" gap={2} css={{ minWidth: 0 }}>
        <Text size="1" css={{ color: '$textSubtle', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.06em' }}>
          {label}
        </Text>
        <Text css={{ fontSize: '$8', lineHeight: 1, fontWeight: 700 }}>{formatNumber(value)}</Text>
        <Text size="1" css={{ color: '$textSubtle' }}>{hint}</Text>
      </Flex>
      <Flex
        align="center"
        justify="center"
        css={{ width: 40, height: 40, flexShrink: 0, borderRadius: '$3', backgroundColor: `$${tone}3`, color: `$${tone}11` }}
      >
        {icon}
      </Flex>
    </Flex>
  </Card>
)

const StatusCell = ({ label, value, color }: { label: string; value: number; color: string }) => (
  <Flex direction="column" gap={1} css={{ minWidth: 0 }}>
    <Flex align="center" gap={2}>
      <Box css={{ width: 8, height: 8, borderRadius: '50%', backgroundColor: color, flexShrink: 0 }} />
      <Text size="1" css={{ color: '$textSubtle' }}>{label}</Text>
    </Flex>
    <Text css={{ pl: 16, fontSize: '$5', fontWeight: 600 }}>{formatNumber(value)}</Text>
  </Flex>
)

const StreamCard = ({ title, stats, received }: { title: string; stats: MessageStats; received?: boolean }) => {
  const navigate = useNavigate()
  const rate = successRate(stats)

  return (
    <Card css={{ p: 0, overflow: 'hidden' }}>
      <Flex justify="space-between" align="center" css={{ px: '$4', py: '$3', borderBottom: '1px solid $tableRowBorder' }}>
        <Flex align="center" gap={2}>
          <Flex align="center" justify="center" css={{ color: received ? '$green11' : '$blue11' }}>
            {received ? <FiDownload size={18} /> : <FiSend size={18} />}
          </Flex>
          <Text css={{ fontWeight: 700 }}>{title}</Text>
          <Badge variant={received ? 'green' : 'blue'}>{formatNumber(stats.total)}</Badge>
        </Flex>
        <Button
          ghost
          variant="secondary"
          onClick={() => navigate(`/message?type=${received ? 'received' : 'published'}`)}
          css={{ height: 28 }}
        >
          View messages
        </Button>
      </Flex>
      <Flex direction="column" gap={4} css={{ p: '$4' }}>
        <Box>
          <Flex justify="space-between" align="center" css={{ mb: '$2' }}>
            <Text size="1" css={{ color: '$textSubtle' }}>Success rate</Text>
            <Text size="1" css={{ fontWeight: 700 }}>{rate}%</Text>
          </Flex>
          <Box css={{ height: 6, borderRadius: 999, backgroundColor: '$gray4', overflow: 'hidden' }}>
            <Box css={{ width: `${rate}%`, height: '100%', borderRadius: 999, backgroundColor: '$green9' }} />
          </Box>
        </Box>
        <Box
          css={{
            display: 'grid',
            gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
            gap: '$4',
            '@bp2': { gridTemplateColumns: 'repeat(3, minmax(0, 1fr))' },
          }}
        >
          <StatusCell label="Succeeded" value={stats.succeeded} color="$green9" />
          <StatusCell label="Pending" value={stats.pending} color="$orange9" />
          <StatusCell label="Failed" value={stats.failed} color="$red9" />
        </Box>
        <Box css={{ height: 1, backgroundColor: '$tableRowBorder' }} />
        <Box css={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: '$4' }}>
          <Flex direction="column" gap={1}>
            <Text size="1" css={{ color: '$textSubtle' }}>Last activity</Text>
            <Text size="1" css={{ fontWeight: 600 }}>{formatDate(stats.lastActivity)}</Text>
          </Flex>
          <Flex direction="column" gap={1}>
            <Text size="1" css={{ color: '$textSubtle' }}>Oldest backlog</Text>
            <Text size="1" css={{ fontWeight: 600 }}>{formatDate(stats.oldestBacklog)}</Text>
          </Flex>
        </Box>
      </Flex>
    </Card>
  )
}

const Home = () => {
  const { stats, loading, error, updatedAt, refetch, cycleStartedAt, refreshIntervalMs, cycleRefreshInterval } = useDashboardStats()
  const { metas, loading: metasLoading, error: metasError } = useMetas()
  const published = stats?.published
  const received = stats?.received
  const failed = (published?.failed || 0) + (received?.failed || 0)
  const backlog = (published?.pending || 0) + (received?.pending || 0)
  const retries = (published?.retries || 0) + (received?.retries || 0)
  const healthy = !error && failed === 0

  return (
    <Flex direction="column" gap={5}>
      <PageTitle title="Home" />
      <Flex justify="space-between" align="center" gap={3} css={{ flexWrap: 'wrap' }}>
        <Flex direction="column" gap={1}>
          <H1>GAP Dashboard</H1>
          <Text css={{ color: '$textSubtle' }}>Message delivery and runtime overview</Text>
        </Flex>
        <Flex align="center" gap={2}>
          <Badge variant={healthy ? 'green' : 'red'}>
            {error ? 'Unavailable' : healthy ? 'Operational' : 'Needs attention'}
          </Badge>
          {updatedAt && <Text size="1" css={{ color: '$textSubtle' }}>{`Updated ${updatedAt.toLocaleTimeString()}`}</Text>}
          <Flex align="center" css={{ gap: '3px' }}>
            <Button ghost variant="secondary" aria-label="Refresh dashboard" disabled={loading} onClick={refetch} css={{ width: 32, height: 32, px: 0 }}>
              <RefreshProgress startedAt={cycleStartedAt} intervalMs={refreshIntervalMs} />
            </Button>
            <Button
              ghost
              variant="secondary"
              aria-label={`Refresh interval ${refreshIntervalMs / 1000}s`}
              onClick={cycleRefreshInterval}
              css={{ width: 32, height: 32, minWidth: 32, px: 0, fontSize: '$1', fontWeight: 600 }}
            >
              {`${refreshIntervalMs / 1000}s`}
            </Button>
          </Flex>
        </Flex>
      </Flex>

      {error && (
        <Card css={{ p: '$3', borderColor: '$red7', backgroundColor: '$red2' }}>
          <Flex align="center" gap={2} css={{ color: '$red11' }}><FiAlertTriangle /><Text>{error}</Text></Flex>
        </Card>
      )}

      {!error && (
        <Card css={{ p: '$3', borderColor: healthy ? '$green7' : '$red7', backgroundColor: healthy ? '$green2' : '$red2' }}>
          <Flex align="center" gap={3}>
            <Flex css={{ color: healthy ? '$green11' : '$red11' }}>
              {healthy ? <FiCheckCircle size={20} /> : <FiAlertTriangle size={20} />}
            </Flex>
            <Flex direction="column" gap={1}>
              <Text css={{ fontWeight: 700 }}>{healthy ? 'Message delivery is healthy' : `${formatNumber(failed)} failed messages require attention`}</Text>
              <Text size="1" css={{ color: '$textSubtle' }}>
                {backlog ? `${formatNumber(backlog)} messages are currently pending.` : 'There is no active message backlog.'}
              </Text>
            </Flex>
          </Flex>
        </Card>
      )}

      <Box
        css={{
          display: 'grid',
          gridTemplateColumns: 'minmax(0, 1fr)',
          gap: '$3',
          '@bp1': { gridTemplateColumns: 'repeat(2, minmax(0, 1fr))' },
          '@bp2': { gridTemplateColumns: 'repeat(2, minmax(0, 1fr))' },
          '@bp3': { gridTemplateColumns: 'repeat(4, minmax(0, 1fr))' },
          '@bp4': { gridTemplateColumns: 'repeat(4, minmax(0, 1fr))' },
        }}
      >
        <MetricCard label="Published" value={published?.total || 0} hint="Messages created by this node" tone="blue" icon={<FiSend size={20} />} />
        <MetricCard label="Received" value={received?.total || 0} hint="Messages consumed by this node" tone="green" icon={<FiDownload size={20} />} />
        <MetricCard label="Pending" value={backlog} hint="Awaiting completion" tone="orange" icon={<FiClock size={20} />} />
        <MetricCard label="Failed" value={failed} hint="Across both message flows" tone="red" icon={<FiAlertTriangle size={20} />} />
      </Box>

      {loading && !stats ? <Text css={{ color: '$textSubtle' }}>Loading message statistics…</Text> : stats && (
        <Box css={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr)', gap: '$4', '@bp3': { gridTemplateColumns: 'repeat(2, minmax(0, 1fr))' }, '@bp4': { gridTemplateColumns: 'repeat(2, minmax(0, 1fr))' } }}>
          <StreamCard title="Published messages" stats={stats.published} />
          <StreamCard title="Received messages" stats={stats.received} received />
        </Box>
      )}

      <Box css={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1fr)', gap: '$4', '@bp2': { gridTemplateColumns: 'minmax(0, 2fr) minmax(280px, 1fr)' } }}>
        <Card css={{ p: 0, overflow: 'hidden' }}>
          <Flex align="center" gap={2} css={{ px: '$4', py: '$3', borderBottom: '1px solid $tableRowBorder' }}>
            <FiDatabase /><Text css={{ fontWeight: 700 }}>Runtime configuration</Text>
          </Flex>
          {metasError && <Text css={{ p: '$4', color: '$red10' }}>{metasError}</Text>}
          {metasLoading && !metasError && <Text css={{ p: '$4', color: '$textSubtle' }}>Loading configuration…</Text>}
          {!metasLoading && !metasError && metas.map((item) => (
            <Flex key={`${item.type}-${item.plugin}-${item.version}`} justify="space-between" align="center" css={{ px: '$4', py: '$3', borderBottom: '1px solid $tableRowBorder' }}>
              <Flex align="center" gap={2}>
                <FiActivity />
                <Text>{item.type === 'Self' ? 'GAP runtime' : item.type}</Text>
              </Flex>
              <Flex align="center" gap={2}>
                <Badge variant={item.type === 'Self' ? 'blue' : 'green'}>{item.type === 'Self' ? 'GAP' : item.plugin}</Badge>
                <Text size="1" css={{ color: '$textSubtle' }}>{item.version}</Text>
              </Flex>
            </Flex>
          ))}
        </Card>
        <Card css={{ p: '$4' }}>
          <Flex direction="column" gap={3}>
            <Flex align="center" gap={2}><FiRepeat /><Text css={{ fontWeight: 700 }}>Delivery pressure</Text></Flex>
            <Flex justify="space-between"><Text size="1" css={{ color: '$textSubtle' }}>Retry attempts</Text><Text css={{ fontWeight: 700 }}>{formatNumber(retries)}</Text></Flex>
            <Flex justify="space-between"><Text size="1" css={{ color: '$textSubtle' }}>Active backlog</Text><Text css={{ fontWeight: 700 }}>{formatNumber(backlog)}</Text></Flex>
            <Flex justify="space-between"><Text size="1" css={{ color: '$textSubtle' }}>Failed messages</Text><Text css={{ fontWeight: 700, color: failed ? '$red11' : '$green11' }}>{formatNumber(failed)}</Text></Flex>
          </Flex>
        </Card>
      </Box>
    </Flex>
  )
}

export default Home
