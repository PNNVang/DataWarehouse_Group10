import React, { useEffect, useState } from "react";
import { getAll } from "../../api/MartAPI";
import DataTable from "react-data-table-component";

const TableComponent = () => {
    const [data, setData] = useState([])
    const [loading, setLoading] = useState(true)
    useEffect(function () {
        async function fetchData() {
            const result = await getAll();

            setData(result)
            setLoading(false)
        }

        fetchData();
    }, [])
    console.log('data', data)

    const columns = [
        {
            name: "STT",
            selector: (row, index) => index + 1, // STT tự động
            sortable: true,
            width: "70px"
        },
        {
            name: "Con Số",
            selector: row => row.numberValue,
            sortable: true,
            cell: row => <span className="number-badge">{row.numberValue}</span>
        },
        {
            name: "Số Lần Xuất Hiện",
            selector: row => row.totalOccurrences,
            sortable: true,
            cell: row => <strong>{row.totalOccurrences}</strong>
        },
        {
            name: "Tổng Kỳ Quay",
            selector: row => row.totalDraws,
            sortable: true
        },
        {
            name: "Tỷ Lệ (%)",
            selector: row => (row.probability * 100).toFixed(2),
            sortable: true,
            cell: row => (
                <div>
                    {(row.probability * 100).toFixed(2)}%
                    <div className="progress-bar">
                        <div
                            className="progress-fill"
                            style={{ width: `${(row.probability * 100).toFixed(2)}%` }}
                        ></div>
                    </div>
                </div>
            )
        },
        {
            name: "Ngày Xuất Hiện Gần Nhất",
            selector: row => row.lastAppearedDate,
            cell: row => <span className="date-badge">{row.lastAppearedDate}</span>
        },
        {
            name: "Số Ngày Chưa Về",
            selector: row => row.daysSinceLast,
            sortable: true,
            cell: row => (
                <span
                    className={`days-badge ${row.daysSinceLast <= 10
                        ? "days-recent"
                        : row.daysSinceLast <= 30
                            ? "days-old"
                            : "days-long"
                        }`}
                >
                    {row.daysSinceLast} ngày
                </span>
            )
        }
    ];


    return (

        <>
            <div className="card full-width">
                <h2>Bảng Thống Kê Chi Tiết</h2>

                <DataTable
                    columns={columns}
                    data={data}
                    progressPending={loading}
                    pagination
                    highlightOnHover
                    striped
                    customStyles={{
                        rows: { style: { minHeight: "60px", fontSize: "14px" } },
                        headCells: { style: { fontWeight: "bold", background: "#f1f1f1", fontSize: "14px" } }
                    }}
                />
            </div>
        </>
    )
}
export default TableComponent