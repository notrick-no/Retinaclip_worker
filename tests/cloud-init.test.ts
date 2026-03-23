import { describe, it, expect } from 'vitest'
import {
  defaultDockerRegistryServerFromImage,
  mergeDockerInsecureRegistries,
} from '../cloud-init'

describe('cloud-init.ts', () => {
  it('mergeDockerInsecureRegistries 从镜像名推断 host:port 并合并配置', () => {
    expect(
      mergeDockerInsecureRegistries('172.16.0.70:5000/quzimu-container:v4', []),
    ).toEqual(['172.16.0.70:5000'])
    const merged = mergeDockerInsecureRegistries('172.16.0.70:5000/quzimu-container:v4', [
      '10.0.0.1:5000',
    ])
    expect(merged.sort()).toEqual(['10.0.0.1:5000', '172.16.0.70:5000'].sort())
    expect(mergeDockerInsecureRegistries('172.16.0.70:5000/quzimu-container:v4', []).length).toBe(1)
  })

  it('mergeDockerInsecureRegistries 对无端口镜像名不自动添加', () => {
    expect(mergeDockerInsecureRegistries('nginx:latest', ['a:1'])).toEqual(['a:1'])
    expect(mergeDockerInsecureRegistries('docker.io/library/nginx:latest', [])).toEqual([])
  })

  it('defaultDockerRegistryServerFromImage', () => {
    expect(defaultDockerRegistryServerFromImage('172.16.0.70:5000/x:y')).toBe('172.16.0.70:5000')
    expect(defaultDockerRegistryServerFromImage('nginx:latest')).toBe('')
  })
})
