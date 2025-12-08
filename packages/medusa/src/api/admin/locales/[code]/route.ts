import { ContainerRegistrationKeys } from "@medusajs/framework/utils"
import { MedusaRequest, MedusaResponse } from "@medusajs/framework/http"
import { HttpTypes } from "@medusajs/framework/types"

export const GET = async (
  req: MedusaRequest<HttpTypes.AdminLocaleParams>,
  res: MedusaResponse<HttpTypes.AdminLocaleResponse>
) => {
  const query = req.scope.resolve(ContainerRegistrationKeys.QUERY)

  const {
    data: [locale],
  } = await query.graph(
    {
      entity: "locale",
      filters: {
        code: req.params.code,
      },
      fields: req.queryConfig.fields,
    },
    {
      cache: { enable: true },
      throwIfKeyNotFound: true,
    }
  )

  res.status(200).json({ locale })
}
