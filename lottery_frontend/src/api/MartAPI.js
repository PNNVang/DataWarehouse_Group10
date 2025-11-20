
export async function getAll() {

    try {

        const url = 'http://localhost:8080/mart/all'
        const response = await fetch(url)
        if (!response.ok) {
            throw new Error(`Lỗi call api`);
        }
        const data = await response.json()
        return data
    }
    catch {
        console.log("lỗi rồi")
    }
}
export async function getStatistic() {
    try {
        const url = 'http://localhost:8080/mart/statistic'
        const response = await fetch(url)
        if (!response.ok) {
            throw new Error(`Lỗi call api`);
        }
        const data = await response.json()
        return data
    }
    catch {
        console.log("lỗi rồi bạn ơi")
    }


}