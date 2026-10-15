import { Helmet } from 'react-helmet-async'

const PageTitle = ({ title }: { title?: string }) => {
  const pageTitle = title ? `${title} - GAP Dashboard` : 'GAP Dashboard'

  return (
    <Helmet>
      <title>{pageTitle}</title>
    </Helmet>
  )
}

export default PageTitle
