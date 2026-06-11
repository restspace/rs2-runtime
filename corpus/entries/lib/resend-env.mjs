// Side-effect module: configure resend's base URL before its module-scope
// `process.env.RESEND_BASE_URL` read (ESM imports evaluate in order).
process.env.RESEND_BASE_URL = "https://api.resend.test";
