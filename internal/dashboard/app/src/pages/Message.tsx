import { useEffect, useState, type ChangeEvent, type ComponentType, type ReactNode } from 'react'
import {
  AriaTable,
  AriaTbody,
  AriaTd,
  AriaTh,
  AriaThead,
  AriaTr,
  Badge,
  Box,
  Button,
  Card,
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
  FiCopy,
  FiFilter,
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

type MessageType = 'pub' | 'rec'

type MessageFilters = {
  status: MsgStatusValue | ''
  topic: string
  group: string
  created_from: string
  created_to: string
  payload: string
}

const emptyFilters: MessageFilters = {
  status: '',
  topic: '',
  group: '',
  created_from: '',
  created_to: '',
  payload: '',
}

const filterParamKeys = ['status', 'topic', 'group', 'created_from', 'created_to', 'payload'] as const

const readMessageType = (params: URLSearchParams): MessageType => {
  const type = params.get('type')
  return type === 'received' || type === 'rec' ? 'rec' : 'pub'
}

const readFilters = (params: URLSearchParams, msgType: MessageType): MessageFilters => {
  const status = params.get('status')

  return {
    status: statuses.includes(status as MsgStatusValue) ? status as MsgStatusValue : '',
    topic: params.get('topic') || '',
    group: msgType === 'rec' ? params.get('group') || '' : '',
    created_from: params.get('created_from') || '',
    created_to: params.get('created_to') || '',
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

const formatJson = (raw: string) => {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2)
  } catch {
    return raw
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
        <Box css={{ flex: 1, p: '$3', borderRadius: '$3', backgroundColor: '$gray2', border: '1px solid $gray4', overflow: 'auto' }}>
          <Text css={{ whiteSpace: escaped ? 'pre-wrap' : 'pre', wordBreak: escaped ? 'break-all' : 'normal', lineHeight: 1.6 }}>
            {escaped ? escapeJson(payload) : formatJson(payload)}
          </Text>
        </Box>
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
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(10)
  const [copied, setCopied] = useState<string>()
  const [filtersExpanded, setFiltersExpanded] = useState(false)
  const msgType = readMessageType(searchParams)
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
    if (searchParams.get('type') === canonicalType) return

    setSearchParams((current) => {
      const next = new URLSearchParams(current)
      next.set('type', canonicalType)
      if (msgType === 'pub') next.delete('group')
      return next
    }, { replace: true })
  }, [msgType, searchParams, setSearchParams])

  useEffect(() => {
    if (!copied) return
    const timer = window.setTimeout(() => setCopied(undefined), 1500)
    return () => window.clearTimeout(timer)
  }, [copied])

  const params: MsgQueryParams = {
    page,
    per_page: pageSize,
    status: appliedFilters.status || undefined,
    topic: appliedFilters.topic || undefined,
    created_from: toApiDateTime(appliedFilters.created_from),
    created_to: toApiDateTime(appliedFilters.created_to),
    payload: appliedFilters.payload || undefined,
  }

  const recParams = {
    ...params,
    group: appliedFilters.group || undefined,
  }

  const pub = usePubMsgs(params, { enabled: msgType === 'pub' })
  const rec = useRecMsgs(recParams, { enabled: msgType === 'rec' })
  const { msgs, total, loading, error, refetch } = msgType === 'pub' ? pub : rec
  const totalPages = Math.max(1, Math.ceil((total || 0) / pageSize))

  const switchType = (next: MessageType) => {
    const currentFilters = readFilters(searchParams, msgType)
    const nextFilters: MessageFilters = {
      ...currentFilters,
      status: '',
      group: next === 'pub' ? '' : currentFilters.group,
    }

    setPage(1)
    setFilters(nextFilters)
    setSearchParams((current) => {
      const params = new URLSearchParams(current)
      params.set('type', next === 'pub' ? 'published' : 'received')
      writeFilters(params, nextFilters, next)
      return params
    }, { replace: true })
  }

  const applyFilters = () => {
    const next = new URLSearchParams(searchParams)
    next.set('type', msgType === 'pub' ? 'published' : 'received')
    writeFilters(next, filters, msgType)

    const filtersUnchanged = next.toString() === searchParams.toString()
    if (filtersUnchanged && page === 1) {
      refetch()
      return
    }

    setPage(1)
    if (!filtersUnchanged) setSearchParams(next, { replace: true })
  }

  const hasFilters = Object.values(filters).some(Boolean)

  return (
    <Flex direction="column" gap={4}>
      <PageTitle title="Messages" />
      <Flex justify="space-between" align="center" gap={3} css={{ flexWrap: 'wrap' }}>
        <H1>Messages</H1>
        <Flex gap={2}>
          <Box
            as="button"
            type="button"
            aria-label="Refresh messages"
            disabled={loading}
            onClick={refetch}
            css={{
              appearance: 'none',
              width: '$6',
              height: '$6',
              p: 0,
              border: 0,
              backgroundColor: 'transparent',
              color: '$buttonPrimaryBg',
              display: 'inline-flex',
              alignItems: 'center',
              justifyContent: 'center',
              cursor: 'pointer',
              '&:hover': { opacity: 0.8 },
              '&:focus-visible': { outline: '2px solid $focusOutline', borderRadius: '$2' },
              '&:disabled': { opacity: 0.45, cursor: 'default' },
            }}
          >
            <MdRefresh size={24} aria-hidden />
          </Box>
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
            px: '$3',
            py: '$2',
            backgroundColor: '$tableHeaderLayerBackground',
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
            rowGap: '$3',
            p: '$3',
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
                    <Badge
                      variant="gray"
                      css={{
                        backgroundColor: 'rgba(0, 0, 0, 0.12)',
                        borderColor: 'rgba(0, 0, 0, 0.55)',
                        color: '$slate12',
                      }}
                    >
                      Any status
                    </Badge>
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
                      <Text size="1">Any status</Text>
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
        </Box>
        <Flex justify="end" css={{ px: '$3', pb: '$3' }}>
          <Button variant="primary" onClick={applyFilters} aria-label="Search messages">
            <FiSearch aria-hidden />
          </Button>
        </Flex>
          </Box>
        )}
      </Card>
      {error && <Text css={{ color: '$red10' }}>{error}</Text>}
      {loading && !error && <Text css={{ color: '$textSubtle' }}>Loading…</Text>}
      {!loading && !error && msgs.length === 0 && <Text css={{ color: '$textSubtle' }}>No messages</Text>}
      <AriaTable
        hasCollapsibleChildren
        css={{
          display: 'grid',
          gridTemplateColumns: msgType === 'rec'
            ? 'max-content minmax(0, 2fr) max-content max-content max-content max-content minmax(0, 1.4fr) max-content'
            : 'max-content minmax(0, 2fr) max-content max-content max-content max-content minmax(0, 1.4fr)',
          '& [role="rowgroup"]': { display: 'contents' },
          '& [role="row"]': { display: 'contents' },
          '& [role="columnheader"]': { backgroundColor: '$tableHeaderLayerBackground' },
          '& [role="rowgroup"] > div:has(> span:not([role]))': {
            display: 'block',
            gridColumn: '1 / -1',
            width: '100%',
          },
          '& [role="rowgroup"] > div:has(> span:not([role])) > span:not([role])': { display: 'none' },
          '& [role="rowgroup"] > div:has(> span:not([role])) > span[role="cell"]': {
            position: 'static',
            display: 'block',
            width: '100%',
            height: 'auto',
          },
          '& [role="rowgroup"] > div:has(> span:not([role])) > span > div': {
            paddingTop: 0,
            paddingBottom: 0,
          },
        }}
      >
          <AriaThead>
            <AriaTr emptyFirstColumn tableHead>
              <AriaTh>ID</AriaTh>
              <AriaTh>Version</AriaTh>
              <AriaTh>CreatedAt</AriaTh>
              <AriaTh>Status</AriaTh>
              <AriaTh>Retries</AriaTh>
              <AriaTh>Topic</AriaTh>
              {msgType === 'rec' && <AriaTh>Group</AriaTh>}
            </AriaTr>
          </AriaThead>
          <AriaTbody>
            {msgs.map((row) => (
              <AriaTr key={row.id} collapsedContent={<MessageBody headers={row.headers} payload={row.payload} />}>
                <AriaTd>
                  <Flex align="center" gap={2}>
                    <Tooltip open={copied === row.id} content="Copied">
                      <Button
                        ghost
                        aria-label="copy id"
                        css={{ height: 20, minHeight: 20, lineHeight: 1, px: '$2' }}
                        onClick={() => {
                          navigator.clipboard.writeText(row.id)
                          setCopied(row.id)
                        }}
                      >
                        <FiCopy />
                      </Button>
                    </Tooltip>
                    <Text>{row.id}</Text>
                  </Flex>
                </AriaTd>
                <AriaTd>
                  <Badge variant="blue">{row.version}</Badge>
                </AriaTd>
                <AriaTd>{row.createdAt}</AriaTd>
                <AriaTd>
                  <Badge variant={statusVariant[row.status]}>{row.status}</Badge>
                </AriaTd>
                <AriaTd>{row.retries}</AriaTd>
                <AriaTd>{row.topic}</AriaTd>
                {msgType === 'rec' && <AriaTd>{row.group}</AriaTd>}
              </AriaTr>
            ))}
          </AriaTbody>
        </AriaTable>
      <Flex justify="space-between" align="center">
        <Flex align="center" gap={2}>
          <Text css={{ color: '$textSubtle' }}>Rows</Text>
          <Select
            value={pageSize}
            onChange={(event: ChangeEvent<HTMLSelectElement>) => {
              setPageSize(Number(event.target.value))
              setPage(1)
            }}
          >
            {[10, 20, 50, 100].map((size) => (
              <option key={size} value={size}>
                {size}
              </option>
            ))}
          </Select>
        </Flex>
        <Flex align="center" gap={2}>
          <Button ghost disabled={page <= 1} aria-label="previous page" onClick={() => setPage((current) => current - 1)}>
            <FiChevronLeft />
          </Button>
          <Text css={{ color: '$textSubtle' }}>{`Page ${page} of ${totalPages}`}</Text>
          <Button
            ghost
            disabled={page >= totalPages}
            aria-label="next page"
            onClick={() => setPage((current) => current + 1)}
          >
            <FiChevronRight />
          </Button>
        </Flex>
      </Flex>
    </Flex>
  )
}

export default Message
