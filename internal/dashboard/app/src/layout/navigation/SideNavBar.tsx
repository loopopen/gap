import {
  Box,
  DialogTitle,
  elevationVariants,
  Flex,
  NavigationLink,
  SidePanel,
  styled,
  Text,
  Tooltip,
  VisuallyHidden,
} from '@traefik-labs/faency'
import { type MouseEvent, useMemo } from 'react'
import { BsChevronDoubleLeft, BsChevronDoubleRight } from 'react-icons/bs'
import { matchPath, useHref, useLocation, useNavigate } from 'react-router-dom'
import { useWindowSize } from 'usehooks-ts'

import Container from '../Container'
import { LAPTOP_BP } from '.'
import { navSections, type NavItem } from '../../config/routes'

const NavigationDrawer = styled(Flex, {
  width: '100%',
  maxWidth: '100%',
  height: 64,
  p: 0,
  variants: {
    elevation: elevationVariants,
  },
  defaultVariants: {
    elevation: 1,
  },
})

const BasicNavigationItem = ({
  route,
  isSmallScreen,
  isExpanded,
  onSidePanelToggle,
}: {
  route: NavItem
  isSmallScreen: boolean
  isExpanded: boolean
  onSidePanelToggle: (isOpen: boolean) => void
}) => {
  const { pathname } = useLocation()
  const navigate = useNavigate()
  const href = useHref(route.path)

  const isActiveRoute = useMemo(() => {
    return matchPath({ path: route.path, end: true }, pathname) != null
  }, [pathname, route.path])

  const onClick = (event: MouseEvent<HTMLAnchorElement>) => {
    event.preventDefault()
    if (isSmallScreen) {
      onSidePanelToggle(false)
    }
    navigate(route.path)
  }

  if (isSmallScreen && !isExpanded) {
    return (
      <Tooltip content={<Text css={{ color: '$tooltipText' }}>{route.label}</Text>} side="right">
        <Box>
          <NavigationLink active={isActiveRoute} startAdornment={route.icon} href={href} onClick={onClick} />
        </Box>
      </Tooltip>
    )
  }

  return (
    <NavigationLink
      onClick={onClick}
      active={isActiveRoute}
      startAdornment={route.icon}
      css={{ whiteSpace: 'nowrap' }}
      href={href}
    >
      {route.label}
    </NavigationLink>
  )
}

export const SideBarPanel = ({
  isOpen,
  onOpenChange,
}: {
  isOpen: boolean
  onOpenChange: (isOpen: boolean) => void
}) => {
  const windowSize = useWindowSize()

  return (
    <SidePanel
      open={isOpen && windowSize.width <= LAPTOP_BP}
      onOpenChange={onOpenChange}
      side="left"
      css={{ width: 264, p: 0 }}
      description="Expanded side navigation"
      noOverlay
      noCloseIcon
    >
      <VisuallyHidden>
        <DialogTitle>side navigation</DialogTitle>
      </VisuallyHidden>
      <SideNav isExpanded={isOpen} onSidePanelToggle={() => onOpenChange(false)} />
    </SidePanel>
  )
}

export const SideNav = ({
  isExpanded,
  onSidePanelToggle,
  isResponsive = false,
}: {
  isExpanded: boolean
  onSidePanelToggle: () => void
  isResponsive?: boolean
}) => {
  const windowSize = useWindowSize()
  const homeHref = useHref('/')
  const navigate = useNavigate()
  const isSmallScreen = windowSize.width <= LAPTOP_BP
  const isSmallAndResponsive = isSmallScreen && isResponsive

  return (
    <NavigationDrawer
      data-collapsed={isExpanded && isSmallAndResponsive}
      css={{
        width: 264,
        height: '100vh',
        position: 'fixed',
        [`@media (max-width:${LAPTOP_BP}px)`]: isResponsive
          ? {
              width: 64,
              'a > div:nth-child(1)': {
                marginLeft: 0,
                paddingRight: 0,
              },
            }
          : undefined,
        transition: '150ms cubic-bezier(0.22, 1, 0.36, 1)',
        '&[data-collapsed="true"]': {
          marginLeft: -32,
        },
      }}
    >
      <button
        type="button"
        aria-label={isExpanded ? 'collapse navigation' : 'expand navigation'}
        onClick={onSidePanelToggle}
        style={{
          display: 'none',
          position: 'absolute',
          top: 8,
          right: isExpanded ? 12 : 16,
          background: 'transparent',
          border: 0,
          color: 'inherit',
          cursor: 'pointer',
        }}
        className="nav-collapse"
      >
        {isExpanded ? <BsChevronDoubleLeft size={16} /> : <BsChevronDoubleRight size={16} />}
      </button>
      <Container
        css={{
          overflow: 'auto',
          p: '$3',
          m: 0,
          flexDirection: 'column',
          [`@media (max-width:${LAPTOP_BP}px)`]: isResponsive ? { p: '$2' } : undefined,
        }}
        data-testid="nav-container"
      >
        <Flex
          as="a"
          gap={2}
          css={{
            color: '$primary',
            mt: '$3',
            mb: '$6',
            textDecoration: 'none',
            height: 'fit-content',
            pl: '$3',
            [`@media (max-width:${LAPTOP_BP}px)`]: isResponsive
              ? { mt: '$6', px: 0, justifyContent: 'center' }
              : undefined,
          }}
          href={homeHref}
          onClick={(event: MouseEvent<HTMLAnchorElement>) => {
            event.preventDefault()
            navigate('/')
          }}
          data-testid="gap-main-nav"
        >
          <Text
            css={{
              fontWeight: 700,
              letterSpacing: '0.08em',
              fontSize: isSmallAndResponsive ? '$3' : '$6',
              lineHeight: 1,
            }}
          >
            GAP
          </Text>
        </Flex>
        {navSections.map((section, index) => (
          <Flex
            key={`nav-section-${index}`}
            direction="column"
            gap="1"
            css={{
              '&:not(:last-child)': {
                borderTop: section.sectionLabel ? '1px solid $colors$tableRowBorder' : undefined,
                mb: '$3',
                pt: section.sectionLabel ? '$3' : undefined,
              },
            }}
          >
            {section.sectionLabel && (
              <Text
                css={{
                  fontWeight: 600,
                  color: '$grayBlue9',
                  mb: '$2',
                  textTransform: 'uppercase',
                  letterSpacing: 0.2,
                  ml: 15,
                  [`@media (max-width:${LAPTOP_BP}px)`]: isResponsive ? { display: 'none' } : undefined,
                }}
              >
                {section.sectionLabel}
              </Text>
            )}
            {section.items.map((item) => (
              <BasicNavigationItem
                key={item.path}
                route={item}
                isSmallScreen={isSmallScreen}
                isExpanded={isExpanded}
                onSidePanelToggle={onSidePanelToggle}
              />
            ))}
          </Flex>
        ))}
      </Container>
      <style>{`
        @media (max-width: ${LAPTOP_BP}px) {
          .nav-collapse { display: inherit !important; }
        }
      `}</style>
    </NavigationDrawer>
  )
}
