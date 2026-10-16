import {
  Button,
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuPortal,
  DropdownMenuTrigger,
  Flex,
  Link,
  Text,
} from '@traefik-labs/faency'
import { FiBookOpen, FiGithub, FiHelpCircle } from 'react-icons/fi'

import ThemeSwitcher from '../../components/ThemeSwitcher'

export const TopNav = () => {
  return (
    <Flex as="nav" role="navigation" justify="space-between" align="center" css={{ mb: '$6' }}>
      <div />
      <Flex gap={2} align="center">
        <ThemeSwitcher />
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button ghost variant="secondary" css={{ px: '$2', boxShadow: 'none' }} aria-label="Help" data-testid="help-menu">
              <FiHelpCircle size={20} />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuPortal>
            <DropdownMenuContent align="end" css={{ zIndex: 9999 }}>
              <DropdownMenuGroup>
                <DropdownMenuItem css={{ height: '$6', cursor: 'pointer' }}>
                  <Link
                    href="https://github.com/loopopen/gap"
                    target="_blank"
                    css={{ textDecoration: 'none', '&:hover': { textDecoration: 'none' } }}
                  >
                    <Flex align="center" gap={2}>
                      <FiBookOpen size={20} />
                      <Text>Repository</Text>
                    </Flex>
                  </Link>
                </DropdownMenuItem>
                <DropdownMenuItem css={{ height: '$6', cursor: 'pointer' }}>
                  <Link
                    href="https://github.com/loopopen/gap"
                    target="_blank"
                    css={{ textDecoration: 'none', '&:hover': { textDecoration: 'none' } }}
                  >
                    <Flex align="center" gap={2}>
                      <FiGithub size={20} />
                      <Text>GitHub</Text>
                    </Flex>
                  </Link>
                </DropdownMenuItem>
              </DropdownMenuGroup>
            </DropdownMenuContent>
          </DropdownMenuPortal>
        </DropdownMenu>
      </Flex>
    </Flex>
  )
}
