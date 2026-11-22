'use strict';
const transform = {'^.+\\.ts$': ['ts-jest', {tsconfig: '<rootDir>/tsconfig.json'}]};
const setupFiles = ['<rootDir>/test/support/silence.ts'];
module.exports = {
  maxWorkers: 1,
  projects: [
    {displayName: 'unit', testEnvironment: 'node', testMatch: ['<rootDir>/test/unit/**/*.test.ts'], transform, setupFiles},
    {displayName: 'integration', testEnvironment: 'node', testMatch: ['<rootDir>/test/integration/**/*.test.ts'], transform, setupFiles},
    {displayName: 'e2e', testEnvironment: 'node', testMatch: ['<rootDir>/test/e2e/**/*.e2e-spec.ts'], transform, setupFiles},
  ],
  collectCoverageFrom: ['src/**/*.ts'],
};
