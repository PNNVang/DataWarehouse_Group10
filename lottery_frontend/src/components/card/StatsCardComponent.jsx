import React, { useEffect, useState } from "react";
import { getStatistic } from "../../api/MartAPI";

const StatsCard = () => {
    const [statisticFill, setStatisticFill] = useState({
        totalOccurrences: '',
        mostNumber: '',
        leastNumber: '',
        lastUpdate: ''
    })
    useEffect(function () {
        async function statistic() {
            const result = await getStatistic()
            console.log('réult', result)
            setStatisticFill(result)

        }
        statistic()
    }, [])
    console.log("state nhận dc", statisticFill)
    return (
        <>
            <div className="stats-cards">
                <div className="stat-card">
                    <h3>Tổng Số Ngày Đã Quay</h3>
                    <div className="value">{statisticFill.totalOccurrences}</div>
                    <div className="label">ngày quay thưởng</div>
                </div>
                <div className="stat-card">
                    <h3>CON SỐ NHIỀU NHẤT</h3>
                    <div className="value">{statisticFill.mostNumber}</div>
                    <div className="label">Xuất hiện nhiều nhất</div>
                </div>
                <div className="stat-card">
                    <h3>CON SỐ ÍT NHẤT</h3>
                    <div className="value">{statisticFill.leastNumber}</div>
                    <div className="label">Xuất hiện ít nhất</div>
                </div>
                <div className="stat-card">
                    <h3>Cập Nhật Gần Nhất</h3>
                    <div className="value">{statisticFill.lastUpdate}</div>
                    <div className="label">Là ngày gần nhất</div>
                </div>
            </div>
        </>
    )
}
export default StatsCard