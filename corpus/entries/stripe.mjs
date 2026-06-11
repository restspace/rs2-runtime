// G5 corpus: stripe — create a customer through the official SDK using its
// fetch HTTP client (the worker/web platform build).
import Stripe from "stripe";

export default async (msg, ctx) => {
  const stripe = new Stripe("sk_test_123", {
    httpClient: Stripe.createFetchHttpClient(),
    host: "api.stripe.test",
    maxNetworkRetries: 0,
  });
  const customer = await stripe.customers.create({ email: "ada@example.com" });
  return {
    status: 200,
    body: { sdk: "stripe", id: customer.id, email: customer.email, object: customer.object },
  };
};
