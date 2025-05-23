const { createApp } = Vue

createApp({
    data() {
        return {
            health: { status: 'unknown' },
            backups: {
                items: [],
                total: 0,
                page: 1,
                page_size: 10,
                total_pages: 1
            },
            logs: []
        }
    },
    methods: {
        async fetchData() {
            try {
                const [healthRes, backupsRes, logsRes] = await Promise.all([
                    fetch('/api/health'),
                    fetch(`/api/backups?page=${this.backups.page}&page_size=${this.backups.page_size}`),
                    fetch('/api/logs')
                ])
                this.health = await healthRes.json()
                this.backups = await backupsRes.json()
                const logsData = await logsRes.json()
                this.logs = logsData.logs
            } catch (error) {
                console.error('Error fetching data:', error)
            }
        },
        async changePage(page) {
            if (page >= 1 && page <= this.backups.total_pages) {
                this.backups.page = page
                await this.fetchData()
            }
        },
        formatSize(bytes) {
            const units = ['B', 'KB', 'MB', 'GB']
            let size = bytes
            let unitIndex = 0
            while (size >= 1024 && unitIndex < units.length - 1) {
                size /= 1024
                unitIndex++
            }
            return `${size.toFixed(2)} ${units[unitIndex]}`
        },
        formatDate(isoDate) {
            return new Date(isoDate).toLocaleString('zh-CN')
        }
    },
    mounted() {
        this.fetchData()
        setInterval(this.fetchData, 30000) // 每30秒更新一次
    }
}).mount('#app')