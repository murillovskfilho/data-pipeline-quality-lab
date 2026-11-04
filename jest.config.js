/** @type {import('jest').Config} */
const shared = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  setupFilesAfterEnv: ['<rootDir>/tests/support/teardown.ts'],
};

module.exports = {
  // Eventually consistent pipeline: tests poll, so give them room.
  testTimeout: 90_000,
  maxWorkers: 4,
  globalSetup: '<rootDir>/tests/support/global-setup.ts',
  projects: [
    { ...shared, displayName: 'e2e', testMatch: ['<rootDir>/tests/e2e/**/*.test.ts'] },
    { ...shared, displayName: 'dq', testMatch: ['<rootDir>/tests/dq/**/*.test.ts'] },
  ],
};
