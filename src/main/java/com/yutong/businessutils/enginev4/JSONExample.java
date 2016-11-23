package com.yutong.businessutils.enginev4;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.lang.StringUtils;
import com.yutong.dbutils.ConnectionUtils;
import com.yutong.poiutils.ExcelUtils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;


public class JSONExample {

    private static Map<String, List<JSONObject>> fieldMap =
            new HashMap<String, List<JSONObject>>();


    public static void main(String[] args) {
        String filePath = "D:\\temp\\03.框架验证系统\\04. BD系统设计\\规则字段映射表.xls";
        List<JSONArray> sheetList = ExcelUtils.readExcel(filePath);
        /* 取得第一个sheet “字段代码对照表” */
        JSONArray jsonArray = sheetList.get(0);

        /* 第一行是标题,去除 */
        for (int i = 1; i < jsonArray.size(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            int rowNum = i + 1;
            String CString = jsonObject.getString("C" + rowNum); // 字段属性名称
            String EString = jsonObject.getString("E" + rowNum); // 有效标识
            String GString = jsonObject.getString("G" + rowNum); // 块级标识
            String JString = jsonObject.getString("J" + rowNum); // 参考映射字段

            /* 排除无效字段 */
            if (StringUtils.equals(EString, "0")) {
                continue;
            }

            JSONObject buff = new JSONObject();
            buff.put("code", CString);
            buff.put("field", JString);

            if (fieldMap.containsKey(GString)) {
                fieldMap.get(GString).add(buff);
            }
            else {
                List<JSONObject> list = new ArrayList<JSONObject>();
                list.add(buff);
                fieldMap.put(GString, list);
            }
        }
        /* 1.字段关系映射集合 */
        // System.out.println(fieldMap);

        JSONArray patientJSONArray = new JSONArray();

        String[] ghdjids = new String[] {
            "12794917032"
        };

        /* 1.1.组织患者信息 */
        JSONObject patient = getCk10GhdjHz(ghdjids[0]);
        if (patient == null) {
            return;
        }

        /* 1.2.组织就诊信息 */
        JSONArray encounterJSONArray = new JSONArray();
        for (int i = 0; i < ghdjids.length; i++) {
            String ghdjid = ghdjids[i];
            JSONObject encounter = getCk10Ghdj(ghdjid);
            if (encounter == null) {
                continue;
            }

            /* 1.3.组织处方信息 */
            JSONArray orderJSONArray = getCk10Cfmx(ghdjid);
            if (orderJSONArray == null) {
                orderJSONArray = new JSONArray();
            }
            encounter.put("orders", orderJSONArray);

            /* 1.4.组织检验报告 */
            JSONArray inspectionJSONArray = new JSONArray();
            encounter.put("inspectionDetails", inspectionJSONArray);

            encounterJSONArray.add(encounter);
        }
        patient.put("encounters", encounterJSONArray);

        patientJSONArray.add(patient);

        System.out.println(patientJSONArray);
    }


    public static JSONObject getCk10GhdjHz(String ghdjid) {
        Connection conn = null;
        JSONObject patient = null;
        try {
            conn = getOracleConnection();
            QueryRunner queryRunner = new QueryRunner();
            Map<String, Object> resultMap = queryRunner.query(conn,
                    query_ck10_ghdj_hz_sql, new MapHandler(), ghdjid);
            if (resultMap == null) {
                return null;
            }

            patient = new JSONObject();
            for (JSONObject json : fieldMap.get("1")) {
                String field = json.getString("field");
                Object filedValue = resultMap.get(field);
                if (filedValue == null) {
                    filedValue = "";
                }
                patient.put(json.get("code"), filedValue);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            DbUtils.closeQuietly(conn);
        }
        return patient;
    }


    public static JSONObject getCk10Ghdj(String ghdjid) {
        Connection conn = null;
        JSONObject encounter = null;
        try {
            conn = getOracleConnection();
            QueryRunner queryRunner = new QueryRunner();
            Map<String, Object> resultMap = queryRunner.query(conn,
                    query_ck10_ghdj_sql, new MapHandler(), ghdjid);
            if (resultMap == null) {
                return null;
            }

            encounter = new JSONObject();
            for (JSONObject json : fieldMap.get("2")) {
                String field = json.getString("field");
                Object filedValue = resultMap.get(field);
                if (filedValue == null) {
                    filedValue = "";
                }
                encounter.put(json.get("code"), filedValue);
            }

            /* 组织多诊断 */
            JSONArray diagnoseArray = getCk10GhdjDzd(ghdjid);
            if (diagnoseArray == null) {
                diagnoseArray = new JSONArray();
            }
            encounter.put("diagnoses", diagnoseArray);

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            DbUtils.closeQuietly(conn);
        }
        return encounter;
    }


    public static JSONArray getCk10GhdjDzd(String ghdjid) {
        Connection conn = null;
        JSONArray diagnoseArray = null;
        try {
            conn = getOracleConnection();
            QueryRunner queryRunner = new QueryRunner();
            List<Map<String, Object>> resultMapList = queryRunner.query(conn,
                    query_ck10_ghdj_dzd_sql, new MapListHandler(), ghdjid,
                    ghdjid, ghdjid, ghdjid, ghdjid, ghdjid, ghdjid);
            if (resultMapList == null) {
                return null;
            }

            diagnoseArray = new JSONArray();
            for (int i = 0; i < resultMapList.size(); i++) {
                JSONObject diagnose = new JSONObject();
                Map<String, Object> resultMap = resultMapList.get(i);
                for (JSONObject json : fieldMap.get("3")) {
                    String field = json.getString("field");
                    Object filedValue = resultMap.get(field);
                    if (filedValue == null) {
                        filedValue = "";
                    }
                    diagnose.put(json.get("code"), filedValue);
                }
                diagnoseArray.add(diagnose);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            DbUtils.closeQuietly(conn);
        }
        return diagnoseArray;
    }


    public static JSONArray getCk10Cfmx(String ghdjid) {
        Connection conn = null;
        JSONArray orderArray = null;
        try {
            conn = getOracleConnection();
            QueryRunner queryRunner = new QueryRunner();
            List<Map<String, Object>> resultMapList = queryRunner.query(conn,
                    query_ck10_cfmx_sql, new MapListHandler(), ghdjid);
            if (resultMapList == null) {
                return null;
            }

            orderArray = new JSONArray();
            for (int i = 0; i < resultMapList.size(); i++) {
                JSONObject order = new JSONObject();
                Map<String, Object> resultMap = resultMapList.get(i);
                for (JSONObject json : fieldMap.get("4")) {
                    String field = json.getString("field");
                    Object filedValue = resultMap.get(field);
                    if (filedValue == null) {
                        filedValue = "";
                    }
                    order.put(json.get("code"), filedValue);
                }

                /* 组织限制类信息 */
                JSONArray limitArray = new JSONArray();
                JSONObject limit = new JSONObject();
                for (JSONObject json : fieldMap.get("5")) {
                    String field = json.getString("field");
                    Object filedValue = resultMap.get(field);
                    if (filedValue == null) {
                        filedValue = "";
                    }
                    limit.put(json.get("code"), filedValue);
                }
                limitArray.add(limit);
                order.put("limitInfos", limitArray);

                orderArray.add(order);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            DbUtils.closeQuietly(conn);
        }
        return orderArray;
    }

    private static String query_ck10_ghdj_dzd_sql = "SELECT '' DZDBSID,\n"
            + "       '' ZDRQ,\n" + "       RYZDDM ZDDM,\n"
            + "       RYZDMC ZDMC,\n" + "       '' ZDNR,\n"
            + "       '' YSID,\n" + "       '0' ZDLX,\n" + "       '' JGID,\n"
            + "       '' JZLB,\n" + "       '' RYBQ,\n" + "       '' JLZT,\n"
            + "       '1' ZDLB\n" + "  FROM CK10_GHDJ_TOTAL G\n"
            + " WHERE ID = ?\n" + "UNION ALL\n" + "SELECT '' DZDBSID,\n"
            + "       '' ZDRQ,\n" + "       CYZDDM ZDDM,\n"
            + "       CYZDMC ZDMC,\n" + "       '' ZDNR,\n"
            + "       '' YSID,\n" + "       '0' ZDLX,\n" + "       '' JGID,\n"
            + "       '' JZLB,\n" + "       '' RYBQ,\n" + "       '' JLZT,\n"
            + "       '2' ZDLB\n" + "  FROM CK10_GHDJ_TOTAL G\n"
            + " WHERE ID = ?\n" + "UNION ALL\n" + "SELECT '' DZDBSID,\n"
            + "       '' ZDRQ,\n" + "       CYZDDM1 ZDDM,\n"
            + "       CYZDMC1 ZDMC,\n" + "       '' ZDNR,\n"
            + "       '' YSID,\n" + "       '1' ZDLX,\n" + "       '' JGID,\n"
            + "       '' JZLB,\n" + "       '' RYBQ,\n" + "       '' JLZT,\n"
            + "       '2' ZDLB\n" + "  FROM CK10_GHDJ_TOTAL G\n"
            + " WHERE ID = ?\n" + "UNION ALL\n" + "SELECT '' DZDBSID,\n"
            + "       '' ZDRQ,\n" + "       CYZDDM2 ZDDM,\n"
            + "       CYZDMC2 ZDMC,\n" + "       '' ZDNR,\n"
            + "       '' YSID,\n" + "       '2' ZDLX,\n" + "       '' JGID,\n"
            + "       '' JZLB,\n" + "       '' RYBQ,\n" + "       '' JLZT,\n"
            + "       '2' ZDLB\n" + "  FROM CK10_GHDJ_TOTAL G\n"
            + " WHERE ID = ?\n" + "UNION ALL\n" + "SELECT '' DZDBSID,\n"
            + "       '' ZDRQ,\n" + "       CYZDDM3 ZDDM,\n"
            + "       CYZDMC3 ZDMC,\n" + "       '' ZDNR,\n"
            + "       '' YSID,\n" + "       '3' ZDLX,\n" + "       '' JGID,\n"
            + "       '' JZLB,\n" + "       '' RYBQ,\n" + "       '' JLZT,\n"
            + "       '2' ZDLB\n" + "  FROM CK10_GHDJ_TOTAL G\n"
            + " WHERE ID = ?\n" + "UNION ALL\n" + "SELECT '' DZDBSID,\n"
            + "       '' ZDRQ,\n" + "       CYZDDM4 ZDDM,\n"
            + "       CYZDMC4 ZDMC,\n" + "       '' ZDNR,\n"
            + "       '' YSID,\n" + "       '4' ZDLX,\n" + "       '' JGID,\n"
            + "       '' JZLB,\n" + "       '' RYBQ,\n" + "       '' JLZT,\n"
            + "       '2' ZDLB\n" + "  FROM CK10_GHDJ_TOTAL G\n"
            + " WHERE ID = ?\n" + "UNION ALL\n" + "SELECT '' DZDBSID,\n"
            + "       '' ZDRQ,\n" + "       CYZDDM5 ZDDM,\n"
            + "       CYZDMC5 ZDMC,\n" + "       '' ZDNR,\n"
            + "       '' YSID,\n" + "       '5' ZDLX,\n" + "       '' JGID,\n"
            + "       '' JZLB,\n" + "       '' RYBQ,\n" + "       '' JLZT,\n"
            + "       '2' ZDLB\n" + "  FROM CK10_GHDJ_TOTAL G\n"
            + " WHERE ID = ?";

    private static String query_ck10_ghdj_hz_sql = "SELECT T.GRID,\n"
            + "       T.GRMC,\n" + "       T.HZXB,\n"
            + "       TO_CHAR((SELECT TO_DATE(NVL(C.CSRQ,'20160101'), 'YYYYMMDD')\n"
            + "          FROM CK01_CBRY C\n"
            + "         WHERE C.ID = T.GRID),'YYYY-MM-DD') CSRQ\n"
            + "  FROM CK10_GHDJ_TOTAL T\n" + " WHERE ID = ?";

    private static String query_ck10_cfmx_sql = "SELECT A.ID,\n"
            + "       A.JYLX,\n" + "       A.GHDJID,\n" + "       A.XMDM,\n"
            + "       TRIM(NVL(B.MC, A.XMMC)) XMMC,\n"
            + "       TRIM(A.YYXMDM) YYXMDM,\n"
            + "       TRIM(A.YYXMMC) YYXMMC,\n"
            + "       TO_CHAR(A.CFRQ,'YYYY-MM-DD HH24:MI:SS') AS CFRQ,\n"
            + "       TO_CHAR(A.JSRQ,'YYYY-MM-DD HH24:MI:SS') AS JSRQ,\n"
            + "       A.YSID,\n" + "       A.YSDM,\n" + "       A.YSMC,\n"
            + "       A.DJ,\n" + "       A.SL AS SL,\n" + "       A.JE AS JE,\n"
            + "       TRIM(NVL(A.GG, B.GG)) AS GG,\n" + "       B.PC AS PC,\n"
            + "       TRIM(A.DW) AS DW,\n" + "       TRIM(A.JX) AS JX,\n"
            + "       TRIM(B.SML_JX) AS SML_JX,\n" + "       B.YL AS YL,\n"
            + "       B.ZHB,\n" + "       TRIM(NVL(A.SFLB, B.SFLB)) AS SFLB,\n"
            + "       B.ZZJGXZ,\n" + "       A.CFH,\n" + "       A.KSID,\n"
            + "       A.KSDM,\n" + "       A.KSMC,\n" + "       A.ZFJE,\n"
            + "       A.ZFBL,\n" + "       B.SMLTYPE,\n" + "       B.SXDM,\n"
            + "       TRIM(B.GZY_DM) GZY_DM,\n"
            + "       TRIM(B.GZY_MC) GZY_MC,\n"
            + "       TRIM(B.GZY_JX) GZY_JX,\n"
            + "       TRIM(B.GZY_GZDM) GZY_GZDM,\n" + "       B.BZ,\n"
            + "       B.YPDJ AS XMDJ,\n" + "       B.ZGJG,\n"
            + "       B.JJDW,\n" + "       B.XJ1,\n" + "       B.XJ2,\n"
            + "       B.XJ3\n" + "  FROM CK10_CFMX_TOTAL A,\n" + "  (\n"
            + "    SELECT DM,MC,DM2 TYDM,TYMC,GG,PC,DW,JX SML_JX,YL,1 ZHB,0 smltype,ZZJGXZ,ZGJG,SXDM,GZY_DM,GZY_MC,GZY_JX,GZY_GZDM,BZ,FL1 XMFL1,FL1 XMFL2,FL3 XMFL3,YPDJ,SFLB,'' JJDW,XJ1,XJ2,XJ3 from CK02_YPML where yxbz='1'\n"
            + "           UNION ALL\n"
            + "    SELECT DM,MC,'' TYDM,''TYMC,'' GG,'' PC,'' DW,'' SML_JX,0 YL,0 ZHB,7 smltype,ZZJGXZ,ZGJG,SXDM,GZY_DM,GZY_MC,'' GZY_JX,GZY_GZDM,BZ,'' XMFL1,'' XMFL2,'' XMFL3,YPDJ,SFLB,JJDW,XJ1,XJ2,XJ3 from CK02_ZLXM where yxbz='1'\n"
            + "           UNION ALL\n"
            + "    SELECT DM,MC,'' TYDM,''TYMC,'' GG,'' PC,'' DW,'' SML_JX,0 YL,0 ZHB,1 smltype,ZZJGXZ,ZGJG,SXDM,GZY_DM,GZY_MC,'' GZY_JX,GZY_GZDM,BZ,'' XMFL1,'' XMFL2,'' XMFL3,YPDJ,SFLB,'' JJDW,XJ1,XJ2,XJ3 from CK02_YYCL where yxbz='1'\n"
            + "  )B\n" + " WHERE A.XMDM = B.DM(+)\n"
            + "   AND A.JE IS NOT NULL\n" + "   AND (A.GHDJID IN ( ? ))\n"
            + " ORDER BY A.CFRQ DESC";

    private static String query_ck10_ghdj_sql = "SELECT G.ID,\n"
            + "       G.GRID,\n" + "       G.HZNL,\n" + "       G.HZXB,\n"
            + "       G.RYLB,\n" + "       G.JGID,\n" + "       G.JGDM,\n"
            + "       G.JGMC,\n" + "       G.CH,\n"
            + "       TO_CHAR(G.CYRQ,'YYYY-MM-DD HH24:MI:SS') CYRQ,\n"
            + "       TO_CHAR(G.RYRQ,'YYYY-MM-DD HH24:MI:SS') RYRQ,\n"
            + "       G.KSID,\n" + "       G.KSDM,\n" + "       G.KSMC,\n"
            + "       G.JZLX,\n" + "       G.YLLB,\n" + "       G.YSID,\n"
            + "       G.YSDM,\n" + "       G.YSMC,\n" + "       G.RYZDDM,\n"
            + "       G.RYZDMC,\n" + "       G.CYZDDM,\n" + "       G.CYZDMC,\n"
            + "       G.CYZDDM1,\n" + "       G.CYZDMC1,\n"
            + "       G.CYZDDM2,\n" + "       G.CYZDMC2,\n"
            + "       G.CYZDDM3,\n" + "       G.CYZDMC3,\n"
            + "       G.CYZDDM4,\n" + "       G.CYZDMC4,\n"
            + "       G.CYZDDM5,\n" + "       G.CYZDMC5,\n"
            + "       G.YCZFY,\n" + "       G.YCZYTS,\n" + "       G.ZYTS,\n"
            + "       G.RYZT,\n" + "       G.DWID,\n" + "       G.SYZT,\n"
            + "       G.TCQDM,\n"
            + "       (SELECT COUNT(0) FROM CK10_JSMX_TOTAL J WHERE J.GHDJID = G.ID) JSMX_COUNT,\n"
            + "       T.*\n" + "  FROM (SELECT A.ID,\n"
            + "               NVL(SUM(B.ZFY), 0) CFMX_ZFY,\n"
            + "               NVL(SUM(B.ZFJE), 0) CFMX_ZFJE,\n"
            + "               NVL(SUM(B.ZFUJE), 0) CFMX_ZFUJE,\n"
            + "               NVL(SUM(ZHZF), 0) GRZHZFJE,\n"
            + "               NVL(SUM(JZJZF), 0) JZJZFJE,\n"
            + "               NVL(SUM(TCZF), 0) TCJZFJE\n"
            + "          FROM CK10_GHDJ_TOTAL A\n"
            + "          LEFT JOIN CK10_JSMX_TOTAL B\n"
            + "            ON A.ID = B.GHDJID\n"
            + "         WHERE A.ID IN ( ? )\n" + "         GROUP BY A.ID) T,\n"
            + "       CK10_GHDJ_TOTAL G\n" + " WHERE T.ID = G.ID\n"
            + " ORDER BY CYRQ ASC";


    public static Connection getOracleConnection() {
        Connection conn = null;
        try {
            conn = ConnectionUtils.getOracleConnection("192.168.1.10", "1521",
                    "orcl", "wxdbs", "wxdbs");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

}
