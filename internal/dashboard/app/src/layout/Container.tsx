import { Flex, styled } from '@traefik-labs/faency'

import breakpoints from '../utils/breakpoints'

const Container = styled(Flex, {
  flexGrow: 1,
  margin: '0 24px',

  [`@media (min-width: ${breakpoints.laptopL})`]: {
    maxWidth: `calc(${breakpoints.laptopL} - 96px)`,
    margin: '0 auto',
  },
})

export default Container
