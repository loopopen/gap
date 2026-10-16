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
  Flex,
  H1,
  Select,
  Text,
  Tooltip,
  Ul,
  Li,
} from '@traefik-labs/faency'
import { FiChevronLeft, FiChevronRight, FiCopy } from 'react-icons/fi'

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
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(10)
  const [status, setStatus] = useState<MsgStatusValue | undefined>()
  const [msgType, setMsgType] = useState<'pub' | 'rec'>('pub')
  const [copied, setCopied] = useState<string>()

  useEffect(() => {
    if (!copied) return
    const timer = window.setTimeout(() => setCopied(undefined), 1500)
    return () => window.clearTimeout(timer)
  }, [copied])

  const params: MsgQueryParams = {
    page,
    per_page: pageSize,
    status,
  }

  const pub = usePubMsgs(params, { enabled: msgType === 'pub' })
  const rec = useRecMsgs(params, { enabled: msgType === 'rec' })
  const { msgs, total, loading, error } = msgType === 'pub' ? pub : rec
  const totalPages = Math.max(1, Math.ceil((total || 0) / pageSize))

  const switchType = (next: 'pub' | 'rec') => {
    setMsgType(next)
    setPage(1)
    setStatus(undefined)
  }

  return (
    <Flex direction="column" gap={4}>
      <PageTitle title="Messages" />
      <H1>Messages</H1>
      <Flex gap={2}>
        <Button variant={msgType === 'pub' ? 'primary' : 'secondary'} onClick={() => switchType('pub')}>
          Published
        </Button>
        <Button variant={msgType === 'rec' ? 'primary' : 'secondary'} onClick={() => switchType('rec')}>
          Received
        </Button>
      </Flex>
      <Flex gap={2} align="center" css={{ flexWrap: 'wrap' }}>
        <Button
          ghost={status !== undefined}
          variant={status === undefined ? 'primary' : 'secondary'}
          onClick={() => {
            setStatus(undefined)
            setPage(1)
          }}
        >
          All
        </Button>
        {statuses.map((item) => (
          <Button
            key={item}
            ghost={status !== item}
            variant={status === item ? 'primary' : 'secondary'}
            onClick={() => {
              setStatus(item)
              setPage(1)
            }}
          >
            {item}
          </Button>
        ))}
      </Flex>
      {error && <Text css={{ color: '$red10' }}>{error}</Text>}
      {loading && !error && <Text css={{ color: '$textSubtle' }}>Loading…</Text>}
      {!loading && !error && msgs.length === 0 && <Text css={{ color: '$textSubtle' }}>No messages</Text>}
      <AriaTable
        hasCollapsibleChildren
        css={{
          display: 'grid',
          gridTemplateColumns: msgType === 'rec'
            ? 'max-content minmax(0, 2fr) max-content max-content max-content minmax(0, 1.4fr) max-content'
            : 'max-content minmax(0, 2fr) max-content max-content max-content minmax(0, 1.4fr)',
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
                  <Badge variant="green">{row.version}</Badge>
                </AriaTd>
                <AriaTd>{row.createdAt}</AriaTd>
                <AriaTd>
                  <Badge variant={statusVariant[row.status]}>{row.status}</Badge>
                </AriaTd>
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
