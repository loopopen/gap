import { Flex, globalCss, styled } from '@traefik-labs/faency'
import { type ReactNode, useState } from 'react'
import { useLocation } from 'react-router-dom'

import Container from './Container'
import PageTitle from './PageTitle'
import { LAPTOP_BP, SideBarPanel, SideNav, TopNav } from './navigation'

export const LIGHT_PRIMARY_COLOR = '#217F97'
export const DARK_PRIMARY_COLOR = '#2AA2C1'

export const globalStyles = globalCss({
  '.light': {
    '--colors-primary': LIGHT_PRIMARY_COLOR,
  },

  '.dark': {
    '--colors-primary': DARK_PRIMARY_COLOR,
  },

  body: {
    backgroundColor: '$contentBg',
    m: 0,
  },
})

const PageContainer = styled(Container, {
  py: '$5',
  px: '$5',
  m: 0,
  '@media (max-width:1440px)': {
    maxWidth: '100%',
  },
})

const Page = ({ children }: { children?: ReactNode }) => {
  const [isSideBarPanelOpen, setIsSideBarPanelOpen] = useState(false)
  const location = useLocation()

  return (
    <>
      {globalStyles()}
      <PageTitle />
      <Flex>
        <SideBarPanel isOpen={isSideBarPanelOpen} onOpenChange={setIsSideBarPanelOpen} />
        <SideNav isExpanded={isSideBarPanelOpen} onSidePanelToggle={() => setIsSideBarPanelOpen(true)} isResponsive />
        <Flex
          justify="center"
          css={{ flex: 1, margin: 'auto', ml: 264, [`@media (max-width:${LAPTOP_BP}px)`]: { ml: 60 } }}
        >
          <PageContainer data-testid={`${location.pathname} page`} direction="column">
            <TopNav />
            {children}
          </PageContainer>
        </Flex>
      </Flex>
    </>
  )
}

export default Page
