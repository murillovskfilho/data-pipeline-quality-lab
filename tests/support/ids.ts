import { randomBytes } from 'node:crypto';

/**
 * Every test owns a fresh tenant, so tests never see each other's rows and can run in
 * parallel against a shared, never-truncated database. Data *content* stays seeded and
 * deterministic; only the namespace is unique.
 */
export const uniqueTenant = (label: string): string =>
  `t_${label}_${Date.now().toString(36)}${randomBytes(3).toString('hex')}`;
