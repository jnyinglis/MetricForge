import { useRef, useEffect, useState, ReactNode } from 'react'

interface ResizablePanelProps {
  children: ReactNode
  defaultWidth: number
  minWidth: number
  maxWidth: number
  side: 'left' | 'right'
  collapsed: boolean
  onCollapse: () => void
  className?: string
}

export function ResizablePanel({
  children,
  defaultWidth,
  minWidth,
  maxWidth,
  side,
  collapsed,
  onCollapse,
  className = '',
}: ResizablePanelProps) {
  const [width, setWidth] = useState(defaultWidth)
  const [isResizing, setIsResizing] = useState(false)
  const panelRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!isResizing || !panelRef.current) return

      const rect = panelRef.current.getBoundingClientRect()
      let newWidth: number

      if (side === 'left') {
        newWidth = e.clientX - rect.left
      } else {
        newWidth = rect.right - e.clientX
      }

      newWidth = Math.max(minWidth, Math.min(maxWidth, newWidth))
      setWidth(newWidth)
    }

    const handleMouseUp = () => {
      setIsResizing(false)
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
    }

    if (isResizing) {
      document.addEventListener('mousemove', handleMouseMove)
      document.addEventListener('mouseup', handleMouseUp)
      document.body.style.cursor = 'col-resize'
      document.body.style.userSelect = 'none'
    }

    return () => {
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
    }
  }, [isResizing, side, minWidth, maxWidth])

  const handleMouseDown = () => {
    setIsResizing(true)
  }

  return (
    <>
      {side === 'left' && (
        <div
          ref={panelRef}
          className={`${className} ${collapsed ? 'collapsed' : ''}`}
          style={{ width: collapsed ? undefined : `${width}px` }}
        >
          <button
            className="sidebar-collapse-btn"
            onClick={onCollapse}
            title={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
          >
            {collapsed ? '▶' : '◀'}
          </button>
          {children}
        </div>
      )}

      {!collapsed && (
        <div
          className={`resize-handle ${isResizing ? 'resizing' : ''}`}
          onMouseDown={handleMouseDown}
        />
      )}

      {side === 'right' && (
        <div
          ref={panelRef}
          className={`${className} ${collapsed ? 'collapsed' : ''}`}
          style={{ width: collapsed ? undefined : `${width}px` }}
        >
          <button
            className="right-panel-collapse-btn"
            onClick={onCollapse}
            title={collapsed ? 'Expand panel' : 'Collapse panel'}
          >
            {collapsed ? '◀' : '▶'}
          </button>
          {children}
        </div>
      )}
    </>
  )
}
