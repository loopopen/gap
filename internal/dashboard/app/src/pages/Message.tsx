import { useEffect, useState, type ChangeEvent, type ComponentType, type ReactNode } from 'react'
import {
  Badge,
  Box,
  Button,
  Card,
  CodeBlock,
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuPortal,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuTrigger,
  Flex,
  H1,
  Input,
  Label,
  Select,
  Text,
  Tooltip,
  Ul,
  Li,
} from '@traefik-labs/faency'
import { useSearchParams } from 'react-router-dom'
import {
  FiChevronDown,
  FiChevronLeft,
  FiChevronRight,
  FiChevronUp,
  FiAlertTriangle,
  FiArrowDown,
  FiArrowUp,
  FiCopy,
  FiFilter,
  FiInbox,
  FiSearch,
  FiX,
} from 'react-icons/fi'
import { MdRefresh } from 'react-icons/md'

import { usePubMsgs, useRecMsgs, type MsgQueryParams } from '../hooks/useMsgs'
import PageTitle from '../layout/PageTitle'
import { MsgStatus, type MsgStatus as MsgStatusValue } from '../types'

const statusVariant: Record<MsgStatusValue, 'green' | 'blue' | 'orange' | 'red'> = {
  [MsgStatus.Succeeded]: 'green',
  [MsgStatus.Processing]: 'blue',
  [MsgStatus.Pending]: 'orange',
  [MsgStatus.Failed]: 'red',
}

const statuses = [MsgStatus.Succeeded, MsgStatus.Processing, MsgStatus.Pending, MsgStatus.Failed]
const pageSizes = [10, 20, 50, 100] as const

type MessageType = 'pub' | 'rec'
type MessageOrder = 'asc' | 'desc'

type MessageFilters = {
  id: string
  status: MsgStatusValue | ''
  topic: string
  group: string
  created_from: string
  created_to: string
  header_key: string
  header_value: string
  payload: string
}

const emptyFilters: MessageFilters = {
  id: '',
  status: '',
  topic: '',
  group: '',
  created_from: '',
  created_to: '',
  header_key: '',
  header_value: '',
  payload: '',
}

const filterParamKeys = ['id', 'status', 'topic', 'group', 'created_from', 'created_to', 'header_key', 'header_value', 'payload'] as const

const readMessageType = (params: URLSearchParams): MessageType => {
  const type = params.get('type')
  return type === 'received' || type === 'rec' ? 'rec' : 'pub'
}

const readMessageOrder = (params: URLSearchParams): MessageOrder => (
  params.get('order') === 'desc' ? 'desc' : 'asc'
)

const readPage = (params: URLSearchParams) => {
  const value = Number(params.get('page'))
  return Number.isInteger(value) && value > 0 ? value : 1
}

const readPageSize = (params: URLSearchParams) => {
  const value = Number(params.get('per_page'))
  return pageSizes.includes(value as typeof pageSizes[number]) ? value : 10
}

const readFilters = (params: URLSearchParams, msgType: MessageType): MessageFilters => {
  const status = params.get('status')

  return {
    id: params.get('id') || '',
    status: statuses.includes(status as MsgStatusValue) ? status as MsgStatusValue : '',
    topic: params.get('topic') || '',
    group: msgType === 'rec' ? params.get('group') || '' : '',
    created_from: params.get('created_from') || '',
    created_to: params.get('created_to') || '',
    header_key: params.get('header_key') || '',
    header_value: params.get('header_value') || '',
    payload: params.get('payload') || '',
  }
}

const writeFilters = (params: URLSearchParams, filters: MessageFilters, msgType: MessageType) => {
  filterParamKeys.forEach((key) => {
    const value = key === 'group' && msgType === 'pub' ? '' : filters[key]
    if (value) params.set(key, value)
    else params.delete(key)
  })
}

const toApiDateTime = (value: string) => {
  if (!value) return undefined
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toISOString()
}

const payloadView = (raw: string) => {
  try {
    return { code: JSON.stringify(JSON.parse(raw), null, 2), isJson: true }
  } catch {
    return { code: raw, isJson: false }
  }
}

const escapeJson = (raw: string) => {
  try {
    return JSON.stringify(JSON.stringify(JSON.parse(raw)))
  } catch {
    return JSON.stringify(raw)
  }
}

const headerEntries = (raw: string) => {
  try {
    const value = JSON.parse(raw) as unknown
    if (!value || typeof value !== 'object' || Array.isArray(value)) return null
    return Object.entries(value).map(([key, item]) => [key, typeof item === 'string' ? item : JSON.stringify(item)] as const)
  } catch {
    return null
  }
}

const headerTone = (key: string) => {
  const name = key.toLowerCase()
  if (name === 'id' || name.endsWith('-id') || name.endsWith('_id')) return 'blue' as const
  if (name.endsWith('-type') || name.endsWith('_type')) return 'purple' as const
  if (name.endsWith('-group') || name.endsWith('_group')) return 'green' as const
  return undefined
}

const SectionLabel = ({ children }: { children: string }) => (
  <Text size="1" css={{ fontWeight: 600, color: '$textSubtle', letterSpacing: '0.06em', textTransform: 'uppercase' }}>
    {children}
  </Text>
)

const HeaderList = Ul as ComponentType<{ css?: Record<string, unknown>; children?: ReactNode }>
const HeaderItem = Li as ComponentType<{ css?: Record<string, unknown>; align?: 'stretch'; children?: ReactNode }>

const MessageBody = ({ headers, payload }: { headers: string; payload: string }) => {
  const entries = headerEntries(headers)
  const [escaped, setEscaped] = useState(false)
  const view = payloadView(payload)
  const displayedPayload = escaped ? escapeJson(payload) : view.code

  return (
    <Flex gap={4} align="stretch" css={{ width: '100%', maxWidth: '100%', minWidth: 0, paddingTop: 16, paddingBottom: 16 }}>
      <Flex direction="column" gap={2} css={{ flex: '6 1 0%', minWidth: 0 }}>
        <Flex align="center" gap={2} css={{ minHeight: 24 }}>
          <SectionLabel>Payload</SectionLabel>
          <Button
            ghost
            variant={escaped ? 'primary' : 'secondary'}
            aria-pressed={escaped}
            aria-label={escaped ? 'show formatted payload' : 'escape payload'}
            onClick={() => setEscaped((current) => !current)}
            css={{ height: 24, px: '$2', fontSize: '$1' }}
          >
            Escape
          </Button>
        </Flex>
        <CodeBlock
          lang={!escaped && view.isJson ? 'json' : 'text'}
          code={displayedPayload}
          copyable
          copyText="Copy"
          copiedText="Copied"
          wrapText={escaped}
          maxHeight={424}
          css={{ flex: 1, minWidth: 0, backgroundColor: '$gray2' }}
        />
      </Flex>
      <Flex direction="column" gap={2} css={{ flex: '4 1 0%', minWidth: 0 }}>
        <Flex align="center" css={{ minHeight: 24 }}>
          <SectionLabel>Headers</SectionLabel>
        </Flex>
        {entries ? (
          <HeaderList css={{ m: 0, listStyle: 'none' }}>
            {entries.map(([key, value]) => {
              const tone = headerTone(key)
              return (
                <HeaderItem
                  key={key}
                  align="stretch"
                  css={{
                    m: 0,
                    borderBottom: '1px solid $tableRowBorder',
                    '&:last-child': { borderBottom: 'none', '& > span > span:first-of-type::after': { bottom: 0 } },
                    '& > span': { p: 0, gap: 0 },
                  }}
                >
                  <Text
                    size="1"
                    css={{
                      flex: '1 1 42%',
                      minWidth: 0,
                      alignSelf: 'stretch',
                      position: 'relative',
                      py: '$2',
                      pr: '$2',
                      color: '$textSubtle',
                      wordBreak: 'break-all',
                      '&::after': {
                        content: '""',
                        position: 'absolute',
                        top: 0,
                        right: 0,
                        bottom: -1,
                        width: 1,
                        backgroundColor: '$tableRowBorder',
                      },
                    }}
                  >
                    {key}
                  </Text>
                  <Text
                    size="1"
                    css={{
                      flex: '1 1 58%',
                      minWidth: 0,
                      alignSelf: 'stretch',
                      py: '$2',
                      pl: '$2',
                      wordBreak: 'break-all',
                      color: tone ? `$${tone}11` : '$textDefault',
                      fontWeight: tone ? 600 : 400,
                    }}
                  >
                    {value}
                  </Text>
                </HeaderItem>
              )
            })}
          </HeaderList>
        ) : (
          <Text size="1" css={{ whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
            {headers || '—'}
          </Text>
        )}
      </Flex>
    </Flex>
  )
}

const Message = () => {
  const [searchParams, setSearchParams] = useSearchParams()
  const [copied, setCopied] = useState<string>()
  const [expandedMessages, setExpandedMessages] = useState<Set<string>>(() => new Set())
  const [filtersExpanded, setFiltersExpanded] = useState(false)
  const msgType = readMessageType(searchParams)
  const messageOrder = readMessageOrder(searchParams)
  const page = readPage(searchParams)
  const pageSize = readPageSize(searchParams)
  const appliedFilters = readFilters(searchParams, msgType)
  const serializedSearchParams = searchParams.toString()
  const [filterDraft, setFilterDraft] = useState(() => ({
    source: serializedSearchParams,
    value: appliedFilters,
  }))
  const filters = filterDraft.source === serializedSearchParams ? filterDraft.value : appliedFilters

  const setFilters = (nextValue: MessageFilters | ((current: MessageFilters) => MessageFilters)) => {
    setFilterDraft((current) => {
      const currentFilters = current.source === serializedSearchParams ? current.value : appliedFilters
      return {
        source: serializedSearchParams,
        value: typeof nextValue === 'function' ? nextValue(currentFilters) : nextValue,
      }
    })
  }

  useEffect(() => {
    const canonicalType = msgType === 'pub' ? 'published' : 'received'
    const canonicalOrder = messageOrder === 'desc' ? 'desc' : null
    const canonicalPage = page === 1 ? null : String(page)
    const canonicalPageSize = pageSize === 10 ? null : String(pageSize)
    if (
      searchParams.get('type') === canonicalType
      && searchParams.get('order') === canonicalOrder
      && searchParams.get('page') === canonicalPage
      && searchParams.get('per_page') === canonicalPageSize
    ) return

    setSearchParams((current) => {
      const next = new URLSearchParams(current)
      next.set('type', canonicalType)
      if (canonicalPage) next.set('page', canonicalPage)
      else next.delete('page')
      if (canonicalPageSize) next.set('per_page', canonicalPageSize)
      else next.delete('per_page')
      if (canonicalOrder) next.set('order', canonicalOrder)
      else next.delete('order')
      if (msgType === 'pub') next.delete('group')
      return next
    }, { replace: true })
  }, [messageOrder, msgType, page, pageSize, searchParams, setSearchParams])

  useEffect(() => {
    if (!copied) return
    const timer = window.setTimeout(() => setCopied(undefined), 1500)
    return () => window.clearTimeout(timer)
  }, [copied])

  const params: MsgQueryParams = {
    page,
    per_page: pageSize,
    id: appliedFilters.id || undefined,
    status: appliedFilters.status || undefined,
    topic: appliedFilters.topic || undefined,
    created_from: toApiDateTime(appliedFilters.created_from),
    created_to: toApiDateTime(appliedFilters.created_to),
    header_key: appliedFilters.header_key || undefined,
    header_value: appliedFilters.header_value || undefined,
    payload: appliedFilters.payload || undefined,
    order: messageOrder === 'desc' ? 'desc' : undefined,
  }

  const recParams = {
    ...params,
    group: appliedFilters.group || undefined,
  }

  const pub = usePubMsgs(params, { enabled: msgType === 'pub' })
  const rec = useRecMsgs(recParams, { enabled: msgType === 'rec' })
  const { msgs, total, loading, error, refetch } = msgType === 'pub' ? pub : rec
  const totalPages = Math.max(1, Math.ceil((total || 0) / pageSize))
  const resultGridColumns = msgType === 'rec'
    ? '32px minmax(300px, 1.2fr) 100px 200px 140px 80px minmax(220px, 0.8fr) minmax(360px, 1.5fr)'
    : '32px minmax(180px, 1.5fr) 90px 180px 110px 70px minmax(180px, 1.3fr)'

  const switchType = (next: MessageType) => {
    const currentFilters = readFilters(searchParams, msgType)
    const nextFilters: MessageFilters = {
      ...currentFilters,
      status: '',
      group: next === 'pub' ? '' : currentFilters.group,
    }

    setFilters(nextFilters)
    setSearchParams((current) => {
      const params = new URLSearchParams(current)
      params.set('type', next === 'pub' ? 'published' : 'received')
      params.delete('page')
      writeFilters(params, nextFilters, next)
      return params
    }, { replace: true })
  }

  const applyFilters = () => {
    const next = new URLSearchParams(searchParams)
    next.set('type', msgType === 'pub' ? 'published' : 'received')
    next.delete('page')
    writeFilters(next, filters, msgType)

    const filtersUnchanged = next.toString() === searchParams.toString()
    if (filtersUnchanged) {
      refetch()
      return
    }

    setSearchParams(next, { replace: true })
  }

  const hasFilters = Object.values(filters).some(Boolean)

  const toggleMessageOrder = () => {
    setSearchParams((current) => {
      const next = new URLSearchParams(current)
      next.delete('page')
      if (messageOrder === 'asc') next.set('order', 'desc')
      else next.delete('order')
      return next
    }, { replace: true })
  }

  const toggleMessage = (id: string) => {
    setExpandedMessages((current) => {
      const next = new Set(current)
      if (next.has(id)) {
        next.delete(id)
      } else {
        next.add(id)
      }
      return next
    })
  }

  return (
    <Flex direction="column" gap={5}>
      <PageTitle title="Messages" />
      <Flex justify="space-between" align="center" gap={3} css={{ flexWrap: 'wrap' }}>
        <Flex direction="column" gap={1}>
          <H1>Messages</H1>
          <Text css={{ color: '$textSubtle' }}>Browse and inspect published and received messages</Text>
        </Flex>
        <Flex align="center" gap={2}>
          <Button variant={msgType === 'pub' ? 'primary' : 'secondary'} onClick={() => switchType('pub')}>
            Published
          </Button>
          <Button variant={msgType === 'rec' ? 'primary' : 'secondary'} onClick={() => switchType('rec')}>
            Received
          </Button>
        </Flex>
      </Flex>
      <Card role="search" aria-label="Message filters" css={{ p: 0, overflow: 'hidden' }}>
        <Flex
          justify="space-between"
          align="center"
          gap={3}
          css={{
            px: '$4',
            py: '$2',
            minHeight: 32,
            backgroundColor: '$gray2',
            borderBottom: '1px solid $tableRowBorder',
          }}
        >
          <Flex align="center" gap={2} css={{ color: '$textDefault' }}>
            <FiFilter aria-hidden />
            <Text css={{ fontWeight: 600 }}>Filters</Text>
          </Flex>
          <Flex align="center" gap={1}>
            <Button
              ghost
              variant="secondary"
              disabled={!hasFilters}
              onClick={() => {
                setFilters(emptyFilters)
              }}
              css={{ height: 28, px: '$2' }}
            >
              <FiX aria-hidden />
              Clear
            </Button>
            <Button
              ghost
              variant="secondary"
              aria-label={filtersExpanded ? 'Collapse filters' : 'Expand filters'}
              aria-expanded={filtersExpanded}
              aria-controls="message-filter-content"
              onClick={() => setFiltersExpanded((current) => !current)}
              css={{ width: 28, height: 28, px: 0, boxShadow: 'none' }}
            >
              {filtersExpanded ? <FiChevronUp aria-hidden /> : <FiChevronDown aria-hidden />}
            </Button>
          </Flex>
        </Flex>
        {filtersExpanded && (
          <Box id="message-filter-content">
        <Box
          css={{
            display: 'grid',
            gridTemplateColumns: 'minmax(0, 1fr)',
            columnGap: '$7',
            rowGap: '$2',
            px: '$3',
            pt: '$2',
            pb: '$2',
            '@bp1': { gridTemplateColumns: 'repeat(2, minmax(0, 1fr))' },
            '@bp2': { gridTemplateColumns: 'repeat(3, minmax(0, 1fr))' },
            '@bp3': { gridTemplateColumns: 'repeat(3, minmax(0, 1fr))' },
            '@bp4': { gridTemplateColumns: 'repeat(3, minmax(0, 1fr))' },
          }}
        >
          <Flex direction="column" gap={1} css={{ minWidth: 0 }}>
            <Label htmlFor="message-filter-status" size="1" weight="semiBold">
              Status
            </Label>
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button
                  id="message-filter-status"
                  variant="secondary"
                  css={{
                    width: '100%',
                    boxSizing: 'border-box',
                    height: '$5',
                    justifyContent: 'space-between',
                    px: '$2',
                    boxShadow: 'none',
                  }}
                >
                  {filters.status ? (
                    <Badge variant={statusVariant[filters.status]}>{filters.status}</Badge>
                  ) : (
                    <Badge variant="gray">All</Badge>
                  )}
                  <FiChevronDown aria-hidden />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuPortal>
                <DropdownMenuContent
                  align="start"
                  sideOffset={4}
                  css={{ width: 'var(--radix-dropdown-menu-trigger-width)', minWidth: 180, zIndex: 9999 }}
                >
                  <DropdownMenuRadioGroup
                    value={filters.status || 'any'}
                    onValueChange={(value: string) => {
                      setFilters((current) => ({
                        ...current,
                        status: value === 'any' ? '' : value as MsgStatusValue,
                      }))
                    }}
                  >
                    <DropdownMenuRadioItem value="any" css={{ height: '$6', cursor: 'pointer' }}>
                      <Badge variant="gray">All</Badge>
                    </DropdownMenuRadioItem>
                    {statuses.map((item) => (
                      <DropdownMenuRadioItem key={item} value={item} css={{ height: '$6', cursor: 'pointer' }}>
                        <Badge variant={statusVariant[item]}>{item}</Badge>
                      </DropdownMenuRadioItem>
                    ))}
                  </DropdownMenuRadioGroup>
                </DropdownMenuContent>
              </DropdownMenuPortal>
            </DropdownMenu>
          </Flex>
          <Flex direction="column" gap={1} css={{ minWidth: 0 }}>
            <Label htmlFor="message-filter-id" size="1" weight="semiBold">
              ID
            </Label>
            <Input
              id="message-filter-id"
              size="small"
              inputMode="numeric"
              pattern="[0-9]*"
              placeholder="Exact message ID"
              value={filters.id}
              onChange={(event) => setFilters((current) => ({ ...current, id: event.target.value }))}
            />
          </Flex>
          <Flex direction="column" gap={1} css={{ minWidth: 0 }}>
            <Label htmlFor="message-filter-topic" size="1" weight="semiBold">
              Topic
            </Label>
            <Input
              id="message-filter-topic"
              size="small"
              placeholder="e.g. topic.greet.hello"
              value={filters.topic}
              onChange={(event) => setFilters((current) => ({ ...current, topic: event.target.value }))}
            />
          </Flex>
          {msgType === 'rec' && (
            <Flex direction="column" gap={1} css={{ minWidth: 0 }}>
              <Label htmlFor="message-filter-group" size="1" weight="semiBold">
                Group
              </Label>
              <Input
                id="message-filter-group"
                size="small"
                placeholder="e.g. notification-workers"
                value={filters.group}
                onChange={(event) => setFilters((current) => ({ ...current, group: event.target.value }))}
              />
            </Flex>
          )}
          <Flex direction="column" gap={1} css={{ minWidth: 0 }}>
            <Label htmlFor="message-filter-header-key" size="1" weight="semiBold">
              Headers
            </Label>
            <Flex align="center" gap={2} css={{ minWidth: 0 }}>
              <Input
                id="message-filter-header-key"
                size="small"
                placeholder="Key"
                value={filters.header_key}
                onChange={(event) => setFilters((current) => ({ ...current, header_key: event.target.value }))}
                css={{ flex: '1 1 0%', minWidth: 0 }}
              />
              <Input
                aria-label="Header value"
                size="small"
                placeholder="Value"
                value={filters.header_value}
                onChange={(event) => setFilters((current) => ({ ...current, header_value: event.target.value }))}
                css={{ flex: '1 1 0%', minWidth: 0 }}
              />
            </Flex>
          </Flex>
          <Flex direction="column" gap={1} css={{ minWidth: 0 }}>
            <Label htmlFor="message-filter-payload" size="1" weight="semiBold">
              Payload
            </Label>
            <Input
              id="message-filter-payload"
              size="small"
              placeholder="Search payload content"
              value={filters.payload}
              onChange={(event) => setFilters((current) => ({ ...current, payload: event.target.value }))}
            />
          </Flex>
          <Flex direction="column" gap={1} css={{ minWidth: 0 }}>
            <Label htmlFor="message-filter-created-from" size="1" weight="semiBold">
              CreatedAt
            </Label>
            <Flex align="center" gap={2} css={{ minWidth: 0 }}>
              <Input
                id="message-filter-created-from"
                aria-label="Created from"
                type="datetime-local"
                size="small"
                value={filters.created_from}
                onChange={(event) => setFilters((current) => ({ ...current, created_from: event.target.value }))}
                css={{ minWidth: 0 }}
              />
              <Text size="1" css={{ color: '$textSubtle', flexShrink: 0 }}>
                to
              </Text>
              <Input
                aria-label="Created to"
                type="datetime-local"
                size="small"
                value={filters.created_to}
                onChange={(event) => setFilters((current) => ({ ...current, created_to: event.target.value }))}
                css={{ minWidth: 0 }}
              />
            </Flex>
          </Flex>
        </Box>
        <Flex justify="end" css={{ px: '$3', pb: '$2' }}>
          <Button variant="primary" onClick={applyFilters} aria-label="Search messages">
            <FiSearch aria-hidden />
          </Button>
        </Flex>
          </Box>
        )}
      </Card>
      {error && (
        <Card css={{ p: '$3', borderColor: '$red7', backgroundColor: '$red2' }}>
          <Flex align="center" gap={2} css={{ color: '$red11' }}><FiAlertTriangle /><Text>{error}</Text></Flex>
        </Card>
      )}

      <Card css={{ p: 0, overflow: 'hidden' }}>
        <Flex
          justify="space-between"
          align="center"
          gap={3}
          css={{
            px: '$4',
            py: '$2',
            minHeight: 32,
            backgroundColor: '$gray2',
            borderBottom: '1px solid $tableRowBorder',
          }}
        >
          <Flex align="center" gap={2} css={{ color: '$textDefault' }}>
            <FiInbox aria-hidden />
            <Text css={{ fontWeight: 700 }}>{msgType === 'pub' ? 'Published messages' : 'Received messages'}</Text>
            <Badge variant={msgType === 'pub' ? 'blue' : 'green'}>{total || 0}</Badge>
          </Flex>
          <Flex align="center" gap={2}>
            {loading && <Text size="1" css={{ color: '$textSubtle' }}>Loading…</Text>}
            <Button
              ghost
              variant="secondary"
              aria-label="Refresh messages"
              disabled={loading}
              onClick={refetch}
              css={{ width: 32, height: 32, px: 0, color: '$buttonPrimaryBg' }}
            >
              <MdRefresh size={20} aria-hidden />
            </Button>
          </Flex>
        </Flex>

        {!loading && !error && msgs.length === 0 ? (
          <Flex direction="column" align="center" gap={2} css={{ py: '$7', color: '$textSubtle' }}>
            <FiInbox size={28} aria-hidden />
            <Text>No messages found</Text>
          </Flex>
        ) : (
          <Box css={{ overflowX: 'auto' }}>
            <Box role="table" aria-label={`${msgType === 'pub' ? 'Published' : 'Received'} messages`} css={{ minWidth: msgType === 'rec' ? 1280 : 970 }}>
              <Box
                role="row"
                css={{
                  display: 'grid',
                  gridTemplateColumns: resultGridColumns,
                  alignItems: 'center',
                  px: '$3',
                  py: '$2',
                  borderBottom: '1px solid $tableRowBorder',
                }}
              >
                <Box role="columnheader" />
                <Flex role="columnheader" align="center" gap={1} css={{ px: '$2', color: '$textSubtle' }}>
                  <Text size="1" css={{ fontWeight: 600 }}>ID</Text>
                  <Tooltip content={messageOrder === 'asc' ? 'Ascending' : 'Descending'}>
                    <Button
                      ghost
                      variant="secondary"
                      aria-label={messageOrder === 'asc' ? 'Sort messages descending' : 'Sort messages ascending'}
                      aria-pressed={messageOrder === 'desc'}
                      onClick={toggleMessageOrder}
                      css={{ width: 24, height: 24, minHeight: 24, px: 0, color: '$buttonPrimaryBg', boxShadow: 'none' }}
                    >
                      {messageOrder === 'asc' ? <FiArrowUp size={14} aria-hidden /> : <FiArrowDown size={14} aria-hidden />}
                    </Button>
                  </Tooltip>
                </Flex>
                {['Version', 'CreatedAt', 'Status', 'Retries', 'Topic'].map((label) => (
                  <Text key={label} role="columnheader" size="1" css={{ px: '$2', color: '$textSubtle', fontWeight: 600 }}>{label}</Text>
                ))}
                {msgType === 'rec' && <Text role="columnheader" size="1" css={{ px: '$2', color: '$textSubtle', fontWeight: 600 }}>Group</Text>}
              </Box>
              <Box role="rowgroup">
                {msgs.map((row) => {
                  const isExpanded = expandedMessages.has(row.id)
                  const cellCss = { minWidth: 0, px: '$2', py: '$3' }

                  return (
                    <Box key={row.id} css={{ borderBottom: '1px solid $tableRowBorder', '&:last-child': { borderBottom: 'none' } }}>
                      <Box
                        role="row"
                        css={{
                          display: 'grid',
                          gridTemplateColumns: resultGridColumns,
                          alignItems: 'center',
                          px: '$3',
                          '&:hover': { backgroundColor: '$gray2' },
                        }}
                      >
                        <Flex role="cell" align="center" css={cellCss}>
                          <Button
                            ghost
                            variant="secondary"
                            aria-label={isExpanded ? `Collapse message ${row.id}` : `Expand message ${row.id}`}
                            aria-expanded={isExpanded}
                            onClick={() => toggleMessage(row.id)}
                            css={{ width: 24, height: 24, minHeight: 24, px: 0, boxShadow: 'none' }}
                          >
                            <FiChevronRight aria-hidden style={{ transition: 'transform 0.2s ease', transform: isExpanded ? 'rotate(90deg)' : 'none' }} />
                          </Button>
                        </Flex>
                        <Flex role="cell" align="center" gap={2} css={cellCss}>
                          <Tooltip open={copied === row.id} content="Copied">
                            <Button
                              ghost
                              variant="secondary"
                              aria-label={`Copy message ID ${row.id}`}
                              css={{ width: 24, height: 24, minHeight: 24, px: 0, boxShadow: 'none', flexShrink: 0 }}
                              onClick={() => {
                                navigator.clipboard.writeText(row.id)
                                setCopied(row.id)
                              }}
                            >
                              <FiCopy aria-hidden />
                            </Button>
                          </Tooltip>
                          <Text size="1" css={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{row.id}</Text>
                        </Flex>
                        <Box role="cell" css={cellCss}><Badge variant="gray">{row.version}</Badge></Box>
                        <Text role="cell" size="1" css={cellCss}>{row.createdAt}</Text>
                        <Box role="cell" css={cellCss}><Badge variant={statusVariant[row.status]}>{row.status}</Badge></Box>
                        <Text role="cell" size="1" css={cellCss}>{row.retries}</Text>
                        <Text role="cell" size="1" css={{ ...cellCss, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{row.topic}</Text>
                        {msgType === 'rec' && <Text role="cell" size="1" css={{ ...cellCss, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{row.group}</Text>}
                      </Box>
                      {isExpanded && (
                        <Box css={{ px: '$4', borderTop: '1px solid $tableRowBorder' }}>
                          <MessageBody headers={row.headers} payload={row.payload} />
                        </Box>
                      )}
                    </Box>
                  )
                })}
              </Box>
            </Box>
          </Box>
        )}

        <Flex justify="space-between" align="center" gap={3} css={{ px: '$4', py: '$3', borderTop: '1px solid $tableRowBorder', flexWrap: 'wrap' }}>
          <Flex align="center" gap={2}>
            <Text size="1" css={{ color: '$textSubtle' }}>Rows</Text>
            <Select
              value={pageSize}
              onChange={(event: ChangeEvent<HTMLSelectElement>) => {
                setSearchParams((current) => {
                  const next = new URLSearchParams(current)
                  next.delete('page')
                  if (event.target.value === '10') next.delete('per_page')
                  else next.set('per_page', event.target.value)
                  return next
                })
              }}
            >
              {pageSizes.map((size) => <option key={size} value={size}>{size}</option>)}
            </Select>
          </Flex>
          <Flex align="center" gap={2}>
            <Button
              ghost
              variant="secondary"
              disabled={page <= 1}
              aria-label="Previous page"
              onClick={() => {
                setSearchParams((current) => {
                  const next = new URLSearchParams(current)
                  if (page - 1 === 1) next.delete('page')
                  else next.set('page', String(page - 1))
                  return next
                })
              }}
            >
              <FiChevronLeft />
            </Button>
            <Text size="1" css={{ color: '$textSubtle' }}>{`Page ${page} of ${totalPages}`}</Text>
            <Button
              ghost
              variant="secondary"
              disabled={page >= totalPages}
              aria-label="Next page"
              onClick={() => {
                setSearchParams((current) => {
                  const next = new URLSearchParams(current)
                  next.set('page', String(page + 1))
                  return next
                })
              }}
            >
              <FiChevronRight />
            </Button>
          </Flex>
        </Flex>
      </Card>
    </Flex>
  )
}

export default Message
