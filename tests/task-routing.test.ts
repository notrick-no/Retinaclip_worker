import { describe, it, expect } from 'vitest'
import { resolveTaskRouting } from '../task-routing'
import type { WorkerConfig } from '../config'

function baseConfig(overrides: Partial<WorkerConfig['processing']> = {}): WorkerConfig {
  return {
    processing: {
      defaultImage: 'registry/default:latest',
      imageMap: {},
      poolProfileMap: {},
      ...overrides,
    },
  } as WorkerConfig
}

describe('task-routing', () => {
  it('processing_image 覆盖映射', () => {
    const cfg = baseConfig({
      imageMap: { 'a@normal': 'wrong:latest' },
    })
    const r = resolveTaskRouting(
      { operation: ['a'], quality_preset: 'normal', processing_image: 'override:1' },
      cfg,
    )
    expect(r.processingImage).toBe('override:1')
    expect(r.resolvedFrom).toBe('message.processing_image')
  })

  it('命中 sortedOps@quality', () => {
    const cfg = baseConfig({
      imageMap: { 'remove subtitles@normal': 'img:sub-n' },
      poolProfileMap: { 'remove subtitles@normal': 'subtitle' },
    })
    const r = resolveTaskRouting(
      { operation: ['remove subtitles'], quality_preset: 'normal' },
      cfg,
    )
    expect(r.processingImage).toBe('img:sub-n')
    expect(r.poolProfile).toBe('subtitle')
    expect(r.resolvedFrom).toBe('imageMap[remove subtitles@normal]')
  })

  it('多 operation 按字典序键', () => {
    const cfg = baseConfig({
      imageMap: { 'b|z@default': 'img:combo' },
    })
    const r = resolveTaskRouting({ operation: ['z', 'b'] }, cfg)
    expect(r.processingImage).toBe('img:combo')
  })

  it('未命中映射时用 defaultImage', () => {
    const cfg = baseConfig({ defaultImage: 'registry/x:y' })
    const r = resolveTaskRouting({ operation: ['unknown'] }, cfg)
    expect(r.processingImage).toBe('registry/x:y')
    expect(r.resolvedFrom).toBe('default')
  })
})
