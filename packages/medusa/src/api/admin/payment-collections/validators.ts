import { z } from "zod"
import { createSelectParams } from "../../utils/validators"

export type AdminGetPaymentCollectionParamsType = z.infer<
  typeof AdminGetPaymentCollectionParams
>
export const AdminGetPaymentCollectionParams = createSelectParams()

export type AdminCreatePaymentCollectionType = z.infer<
  typeof AdminCreatePaymentCollection
>
export const AdminCreatePaymentCollection = z
  .object({
    order_id: z.string(),
    amount: z.number().optional(),
  })
  .strict()
