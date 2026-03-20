import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    environment: 'node',
    include: ['tests/**/*.test.ts'],
    // 由于项目没有类型检查流程，这里不启用 watch/coverage 默认值
  },
})

