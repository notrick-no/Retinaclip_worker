import { describe, it, expect } from 'vitest'
import {
  buildProgressPayload,
  buildCompletedPayload,
  buildFailedPayload,
} from '../webhook-sender'

describe('webhook-sender payload builders', () => {
  it('progress stage 映射为 download_progress / processing_progress / upload_progress', () => {
    const p1 = buildProgressPayload('job-1', 'downloading', 10, 'downloading...')
    expect(p1.event_type).toBe('download_progress')
    expect(p1.current_stage).toBe('downloading')

    const p2 = buildProgressPayload('job-1', 'processing', 50, 'processing...')
    expect(p2.event_type).toBe('processing_progress')
    expect(p2.current_stage).toBe('processing')

    const p3 = buildProgressPayload('job-1', 'uploading', 90, 'uploading...')
    expect(p3.event_type).toBe('upload_progress')
    expect(p3.current_stage).toBe('uploading')
  })

  it('completed / failed payload 基本字段正确', () => {
    const completed = buildCompletedPayload('job-1', 'http://out.mp4', 12.34, 'u1')
    expect(completed.event_type).toBe('completed')
    expect(completed.output_video_url).toBe('http://out.mp4')

    const failed = buildFailedPayload('job-1', 'boom', 3, 'u1')
    expect(failed.event_type).toBe('failed')
    expect(failed.error_message).toBe('boom')
    expect(failed.retry_count).toBe(3)
  })
})

