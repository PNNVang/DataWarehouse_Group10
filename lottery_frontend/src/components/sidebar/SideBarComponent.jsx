import React from "react";

const SideBarComponent = () => {
    return (
        <>
            <div className="sidebar" id="sidebar">
                <div className="sidebar-header">
                    <h2>XSMB</h2>
                    <p>Thống Kê CHI TIẾT</p>
                </div>

                <div className="sidebar-menu">
                    <div className="menu-section">MENU CHÍNH</div>
                    <div className="menu-item active">

                        <span>Dashboard Tổng Quan</span>
                    </div>
                    <div className="menu-item">

                        <span>Tìm kiếm kết quả</span>
                    </div>
                    {/* <!-- <div className="menu-item">

                        <span>Giải Đặc Biệt</span>
                    </div>
                    <div className="menu-item">

                        <span>Phân Tích Tần Suất</span>
                    </div>

                    <div className="menu-section">CÔNG CỤ</div>
                    <div className="menu-item">

                        <span>Tra Cứu Kết Quả</span>
                    </div>
                    <div className="menu-item">

                        <span>Lịch Sử Quay Số</span>
                    </div>
                    <div className="menu-item">

                        <span>Dự Đoán Thông Minh</span>
                    </div>
                    <div className="menu-item">

                        <span>Thông Báo Kết Quả</span>
                    </div>

                    <div className="menu-section">CÀI ĐẶT</div>
                    <div className="menu-item">

                        <span>Cài Đặt</span>
                    </div>
                    <div className="menu-item">

                        <span>Trợ Giúp</span>
                    </div> --> */}
                </div>

                <div className="sidebar-footer">
                    <p>SAU 6H CHIỀU CHƯA BIẾT AI GIÀU HƠN</p>
                    <p style={{ marginTop: "5px", fontSize: "0.75em" }}>Version 1.0.0</p>
                </div>
            </div >
        </>
    )
}

export default SideBarComponent