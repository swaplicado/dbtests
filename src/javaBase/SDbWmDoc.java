/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package javaBase;

import java.sql.Date;
import java.sql.Timestamp;

/**
 *
 * @author Alfredo Pérez
 */
public class SDbWmDoc {

    public SDbWmDoc() {
        initRegistry();
    }

    public void initRegistry() {
        mnId_year = 0;
        mnId_doc = 0;
        mnFid_ct_dps = 0;
        mnFid_cl_dps = 0;
        msNum = "";
        msNum_Ser = "";
        mdWeight = 0.0;
        mbDel = false;
        mbSys = false;
        mdDt = null;
        mtTsEdit = null;
    }

    protected int mnId_year;
    protected int mnId_doc;
    protected int mnFid_ct_dps;
    protected int mnFid_cl_dps;
    protected String msNum;
    protected String msNum_Ser;
    protected double mdWeight;
    protected boolean mbDel;
    protected boolean mbSys;
    protected Date mdDt;
    protected Timestamp mtTsEdit;

    /**
     * Public Methods
     */
    public int getId_year() {
        return mnId_year;
    }

    public Timestamp getTsEdit() {
        return mtTsEdit;
    }

    public Date getDt() {
        return mdDt;
    }

    public int getId_doc() {
        return mnId_doc;
    }

    public int getFid_ct_dps() {
        return mnFid_ct_dps;
    }

    public int getFid_cl_dps() {
        return mnFid_cl_dps;
    }

    public String getNum() {
        return msNum;
    }

    public String getNum_Ser() {
        return msNum_Ser;
    }

    public double getWeight() {
        return mdWeight;
    }

    public boolean getDel() {
        return mbDel;
    }

    public boolean getSys() {
        return mbSys;
    }

    public void setNum(String s) {
        this.msNum = s;
    }

    public void setNum_Ser(String s) {
        this.msNum_Ser = s;
    }

    public void setWeight(double d) {
        this.mdWeight = d;
    }

    public void setDel(boolean b) {
        this.mbDel = b;
    }

    public void setSys(boolean b) {
        this.mbSys = b;
    }

    public void setDt(Date date) {
        this.mdDt = date;
    }

    public void setFid_cl_dps(int n) {
        this.mnFid_cl_dps = n;
    }

    public void setId_year(int n) {
        this.mnId_year = n;
    }

    public void setId_doc(int n) {
        this.mnId_doc = n;
    }

    public void setFid_ct_dps(int n) {
        this.mnFid_ct_dps = n;
    }

    public void setTsEdit(Timestamp ts) {
        this.mtTsEdit = ts;
    }

}
